package kc

import (
	"archive/tar"
	gz "compress/gzip"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreybutler/go-fsutil"
	"github.com/mauricioscastro/kcdump/pkg/yjq"
	"github.com/pieterclaerhout/go-waitgroup"
	"github.com/rwtodd/Go.Sed/sed"
	"go.uber.org/zap"
)

// cmd exec in pod
// 'https://cluster:443/api/v1/namespaces/exec/pods/shell-demo/exec?command=ls&container=loop&stderr=true&stdout=true'

const (
	YAML               int = 0
	JSON               int = 1
	JSON_LINES         int = 2
	JSON_LINES_WRAPPED int = 3
	JSON_PRETTY        int = 4
)

var (
	DefaultCleaningQuery  = `.items = [.items[] | del(.metadata.managedFields) | del(.metadata.uid) | del (.metadata.creationTimestamp) | del (.metadata.generation) | del(.metadata.resourceVersion) | del (.metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"])] | del(.metadata)`
	apiAvailableListQuery = `with(.items[]; .verbs = (.verbs | to_entries)) | .items[] | select(.available and .verbs[].value == "get") | .name + ";" + .groupVersion + ";" + .namespaced`
	dumpWorkerErrors      atomic.Value
)

func (kc *kc) Ns() (string, error) {
	r, e := kc.GetJson("/api/" + kc.Version() + "/namespaces")
	if e != nil {
		return r, e
	}
	r, e = yjq.YqEvalJ2Y(DefaultCleaningQuery, r)
	if e != nil {
		return r, e
	}
	return r, e
}

func (kc *kc) ApiResources() (string, error) {
	r, e := kc.GetJson("/apis")
	if e != nil {
		return "", e
	}
	apisList, e := yjq.Eval2List(yjq.JqEval, `["/api/%s", "/apis/" + .groups[].preferredVersion.groupVersion] | .[]`, r, kc.Version())
	if e != nil {
		return "", e
	}
	var kcList []Kc
	var wg sync.WaitGroup
	for _, api := range apisList {
		_kc := NewKc()
		wg.Add(1)
		go func(api string) {
			defer wg.Done()
			_kc.SetResponseTransformer(apiResourcesResponseTransformer).GetJson(api)
		}(api)
		kcList = append(kcList, _kc)
	}
	wg.Wait()
	var sb strings.Builder
	apisHeader := fmt.Sprintf("kind: APIResourceList\napiVersion: %s\nitems:\n", kc.Version())
	sb.WriteString(apisHeader)
	for i, _kc := range kcList {
		if len(_kc.Response()) == 0 || _kc.Err() != nil {
			continue
		}
		sb.WriteString(_kc.Response())
		if i+1 < len(kcList) {
			sb.WriteString("\n")
		}
	}
	return sb.String(), nil
}

func apiResourcesResponseTransformer(kc Kc) (string, error) {
	var resp string
	err := kc.Err()
	logger.Debug("transforming resource " + kc.Api())
	unavailableApiResp := `[{"groupVersion": "%s", "available": false, "error": "%s"}]`
	api := strings.TrimPrefix(kc.Api(), "/apis/")
	if err != nil {
		logger.Warn("ApiResources bad api", zap.String("api", kc.Api()), zap.Error(err))
		if resp, err = yjq.JqEval2Y(unavailableApiResp, "", api, err.Error()); err != nil {
			logger.Error("ApiResources bad api JqEval2Y", zap.Error(err))
		}
	} else {
		resp, err = yjq.JqEval2Y(`.resources[] += {"groupVersion": .groupVersion} | .resources[] += {"available": true} | [.resources[] | select(.name | contains("/") | not) | del(.storageVersionHash) | del(.singularName)]`, kc.Response())
		if err != nil {
			logger.Warn("ApiResources JqEval2Y", zap.String("api", kc.Api()), zap.Error(err))
			resp, err = yjq.JqEval2Y(unavailableApiResp, "", api, err.Error())
		} else if resp == "[]" {
			logger.Warn("ApiResources empty reply from api", zap.String("api", kc.Api()))
			resp, err = yjq.JqEval2Y(unavailableApiResp, "", api, "empty reply from api")
		}
	}
	return resp, err
}

// Dumps whole cluster information to path. each api resource listed with
// KcApiResources is treated in a separate thread. The pool size for the
// the threads can be expressed through poolsize (0 or -1 to unbound it).
// progress will be called at the end. You need to add thread safety mechanisms
// to the code inside progress func().
func (kc *kc) Dump(path string, nsExclusionList []string, gvkExclusionList []string, nologs bool, gz bool, tgz bool, prune bool, splitns bool, splitgv bool, format int, poolSize int, chunkSize int, progress func()) error {
	_gz := gz
	if tgz || !splitgv { // only gzip in the end after archiving or after concatenating
		_gz = false
	}
	if !splitgv {
		if format != YAML && format != JSON_LINES {
			return fmt.Errorf("not splitting gv needs format to be 'yaml' or 'json_lines'\n")
		}
		splitns = false
		if format == JSON_LINES {
			format = JSON // trick the file writer and put the whole list in one line or else do not extract items.
		}
	}
	dumpDir := strings.Replace(kc.cluster, "https://", "", -1)
	dumpDir = strings.Replace(dumpDir, ":", ".", -1)
	dumpDir = strings.Replace(dumpDir, ".", "_", -1)
	path = path + "/" + dumpDir
	fsutil.Clean(filepath.FromSlash(path))
	// also remove possible old files from different options scenarios
	os.Remove(path + ".tar.gz")
	os.Remove(path + ".json")
	os.Remove(path + ".yaml")
	os.Remove(path + ".json.gz")
	os.Remove(path + ".yaml.gz")
	path = path + "/"
	// big things to retrieve serially
	// name.gv -> chunk size to use
	bigSizedReplyMap := map[string]int{
		"packagemanifests.packages.operators.coreos.com/v1": 1,
		"configmaps.v1": 1,
		"apirequestcounts.apiserver.openshift.io/v1":        5,
		"customresourcedefinitions.apiextensions.k8s.io/v1": 10,
	}
	//
	// retrieve gvk list and write
	//
	apis, err := kc.ApiResources()
	if err != nil {
		return err
	}
	// filter out gvk based on regex list
	if apis, err = FilterApiResources(apis, gvkExclusionList); err != nil {
		return err
	}
	if err = writeResourceFile(path+"api_resources.yaml", apis, _gz, format); err != nil {
		return err
	}
	//
	// retrieve ns list and write
	//
	ns, err := kc.Ns()
	if err != nil {
		return err
	}
	// filter out ns based on regex list
	if ns, err = FilterNS(ns, nsExclusionList, true); err != nil {
		return err
	}
	// append api name
	if ns, err = yjq.YqEval(`.metadata.apiName = "namespaces"`, ns); err != nil {
		return err
	}
	if splitns {
		nsList, err := yjq.Eval2List(yjq.YqEval, ".items[].metadata.name", ns)
		if err != nil {
			return err
		}
		for _, n := range nsList {
			// ns_dir_name := strings.ReplaceAll(n, "-", "_")
			fsutil.Mkdirp(filepath.FromSlash(path + n))
			if !nologs {
				fsutil.Mkdirp(filepath.FromSlash(path + n + "/log"))
			}
		}
	} else if !nologs {
		fsutil.Mkdirp(filepath.FromSlash(path + "/log"))
	}
	if err = writeResourceFile(path+"namespaces."+kc.Version()+".yaml", ns, _gz, format); err != nil {
		return err
	}
	//
	// retrieve everything else
	//
	apiList, err := yjq.Eval2List(yjq.YqEval, apiAvailableListQuery, apis)
	if err != nil {
		return err
	}
	//
	// retrieve big guys first serially
	//
	for i, le := range apiList {
		name, gv, namespaced, baseName := getApiAvailableListQueryValues(kc.Version(), le)
		if bigSizedReplyChunkSize, isBig := bigSizedReplyMap[name+"."+gv]; isBig {
			chunkSize = bigSizedReplyChunkSize
			logger.Info(fmt.Sprintf("%s.%s is considered big and will use chunks of size %d", name, gv, chunkSize))
			writeResourceList(path, baseName, name, gv, namespaced, splitns, nsExclusionList, nologs, _gz, format, chunkSize, progress)
			logger.Info(fmt.Sprintf("%s.%s finished", name, gv))
			apiList = slices.Delete(apiList, i, i+1)
		}
	}
	//
	// retrieve everything else in parallel
	//
	dumpWorkerErrors.Store(make([]error, 0))
	wg := waitgroup.NewWaitGroup(poolSize)
	for _, le := range apiList {
		name, gv, namespaced, baseName := getApiAvailableListQueryValues(kc.Version(), le)
		if name == "namespaces" && gv == kc.Version() {
			continue
		}
		logger.Debug("KcDump", zap.String("baseName", baseName), zap.String("name", name), zap.Bool("namespaced", namespaced), zap.String("gv", gv))
		wg.BlockAdd()
		go func() {
			defer wg.Done()
			writeResourceList(path, baseName, name, gv, namespaced, splitns, nsExclusionList, nologs, _gz, format, chunkSize, progress)
		}()
	}
	wg.Wait()
	if err = joinChunks(path, format, _gz); err != nil {
		return err
	}
	version, err := kc.Get("/version")
	if err != nil {
		return err
	}
	if version, err = yjq.YqEval(`. += {"dumpDate": "%s"}`, version, time.Now().Format(time.RFC3339)); err != nil {
		return err
	}
	if format == JSON_LINES || format == JSON_LINES_WRAPPED {
		format = JSON
	}
	if err = writeResourceFile(path+"version.yaml", version, _gz, format); err != nil {
		return err
	}
	if tgz {
		tgzpath := path[:len(path)-1] + ".tar"
		if err = archive(path, tgzpath); err != nil {
			logger.Error("tape archiving error", zap.Error(err))
			return err
		}
		if err = gzip(tgzpath); err != nil {
			logger.Error("gzip error", zap.Error(err))
		}
		if prune {
			os.RemoveAll(path)
		}
	}
	if !splitgv {
		bigFileName := dumpDir
		if format == YAML {
			bigFileName = bigFileName + ".yaml"
		} else {
			bigFileName = bigFileName + ".json"
		}
		bigFilePath := strings.Replace(path, dumpDir+"/", "", -1) + bigFileName
		logger.Info("joining extracted into a single file " + bigFilePath)
		separator := "\n"
		if format == YAML {
			separator = "\n---\n"
		}
		bigFile, err := os.OpenFile(bigFilePath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		l, err := fsutil.ListFiles(path, false)
		if err != nil {
			return err
		}
		for fi, f := range l {
			if fi > 0 {
				if _, err := bigFile.WriteString(separator); err != nil {
					return err
				}
			}
			if err := copyFile(f, bigFile); err != nil {
				return err
			}
			os.Remove(f)
		}
		bigFile.Close()
		if gz {
			if err = gzip(bigFilePath); err != nil {
				logger.Error("gzip error", zap.Error(err))
			}
			logger.Info("gzipped " + bigFilePath)
		}
		if nologs {
			os.Remove(path)
		}
	}
	if len(dumpWorkerErrors.Load().([]error)) > 0 {
		var collectedErrors strings.Builder
		for _, e := range dumpWorkerErrors.Load().([]error) {
			collectedErrors.WriteString(e.Error())
			collectedErrors.WriteString("\n")
		}
		if collectedErrors.Len() > 0 {
			return errors.New(collectedErrors.String())
		}
	}
	logger.Info("finished dumping cluster " + kc.cluster)
	return nil
}

func getApiAvailableListQueryValues(kcVer string, listEntry string) (string, string, bool, string) {
	le := strings.Split(listEntry, ";")
	name := le[0]
	gv := le[1]
	namespaced, _ := strconv.ParseBool(le[2])
	baseName := "/apis/"
	if gv == kcVer {
		baseName = "/api/"
	}
	return name, gv, namespaced, baseName
}

func writeResourceList(path string, baseName string, name string, gv string, namespaced bool, splitns bool, nsExclusionList []string, nologs bool, gz bool, format int, chunkSize int, progress func()) error {
	if progress != nil {
		defer progress()
	}
	logLine := fmt.Sprintf("writeResourceList: baseName=%s name=%s gv=%s namespaced=%t", baseName, name, gv, namespaced)
	kc := NewKc()
	gvName := strings.ReplaceAll(gv, "/", "_")
	gvName = strings.ReplaceAll(gvName, ".", "_")
	fileName := name + "." + gvName + ".yaml"
	formatResourceContentSep := "\n"
	listKind := ""
	if format == JSON {
		formatResourceContentSep = ","
	} else if format == JSON_PRETTY {
		formatResourceContentSep = ",\n"
	}
	logger.Info("starting " + logLine)
	defer logger.Info("finishing " + logLine)
	// start getting chunked list
	for ctk := ""; ; {
		apiResources, err := kc.
			SetResponseTransformer(apiIgnoreNotFoundResponseTransformer).
			SetGetParam("limit", fmt.Sprintf("%d", chunkSize)).
			SetGetParam("continue", ctk).
			Get(baseName + gv + "/" + name)
		if err != nil {
			return writeResourceListLog("get api chunk "+logLine, err)
		}
		if len(apiResources) == 0 {
			if len(ctk) == 0 {
				break
			}
			continue
		}
		ctk, err = yjq.YqEval(`.metadata.continue // ""`, apiResources)
		if err != nil {
			return writeResourceListLog("find continue get api chunk "+logLine, err)
		}
		ric, err := yjq.Eval2Int(yjq.YqEval, `.metadata.remainingItemCount // "0"`, apiResources)
		if err != nil {
			logger.Debug("Eval2Int get api chunk "+logLine, zap.Error(err))
			ric = 0
		}
		if apiResources, err = cleanApiResourcesChunk(apiResources, name, gv, nsExclusionList); err != nil {
			return writeResourceListLog("cleanApiResourcesChunk "+logLine, err)
		}
		if len(apiResources) > 0 {
			if listKind == "" {
				if listKind, err = yjq.YqEval(".kind", apiResources); err != nil {
					return writeResourceListLog("get chunk list kind "+logLine, err)
				}
			}
			if !namespaced || !splitns {
				if apiResources, err = formatResourceContent(apiResources, format, true); err != nil {
					return writeResourceListLog("formatting resource content  "+logLine, err)
				}
				if err = writeTempChunk(path+listKind+"."+fileName+".chunk", apiResources, formatResourceContentSep); err != nil {
					return writeResourceListLog("writeTempChunk "+logLine, err)
				}
				if !nologs && name == "pods" && gv == kc.Version() {
					if err = writeLogs(kc, path+"log/", apiResources, baseName, gv, gz, splitns, logLine); err != nil {
						return writeResourceListLog("write logs "+logLine, err)
					}
				}
			} else {
				nsList, err := yjq.Eval2List(yjq.YqEval, "[.items[].metadata.namespace] | unique | .[]", apiResources)
				if err != nil {
					return writeResourceListLog("read ns list from apiResources"+logLine, err)
				}
				for _, ns := range nsList {
					// nsPath := path + strings.ReplaceAll(ns, "-", "_") + "/"
					nsPath := path + ns + "/"
					apiByNs, err := yjq.YqEval(`.items = [.items[] | select(.metadata.namespace=="%s")]`, apiResources, ns)
					if err != nil {
						return writeResourceListLog("apiByNs "+logLine, err)
					}
					formattedApiRs, err := formatResourceContent(apiByNs, format, true)
					if err != nil {
						return writeResourceListLog("formatting resource content  "+logLine, err)
					}
					if err = writeTempChunk(nsPath+listKind+"."+fileName+".chunk", formattedApiRs, formatResourceContentSep); err != nil {
						return writeResourceListLog("writeTempChunk "+logLine, err)
					}
					// get pods' logs or no
					if !nologs && name == "pods" && gv == kc.Version() {
						if err = writeLogs(kc, nsPath+"log/", apiByNs, baseName, gv, gz, splitns, logLine); err != nil {
							return writeResourceListLog("write logs "+logLine, err)
						}
					}
				}
			}
		}
		if ric == 0 {
			break
		}
	}
	return nil
}

func joinChunks(path string, format int, gz bool) error {
	logger.Debug("start joining chunks")
	defer logger.Debug("finished joining chunks")
	return filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(p, ".chunk") {
			logger.Debug("joining chunks for " + p)
			finalFile := strings.Replace(p, ".chunk", "", 1)
			if format != YAML {
				finalFile = strings.Replace(finalFile, ".yaml", ".json", 1)
			}
			kind, name, gv := getApiListHeaderFromFileName(finalFile)
			switch format {
			case JSON_LINES, JSON_LINES_WRAPPED:
				if err := fsutil.Move(p, finalFile); err != nil {
					return err
				}
				if gz {
					if err = gzip(finalFile); err != nil {
						return err
					}
				}
			case YAML:
				header := "kind: %s\napiVersion: %s\nmetadata:\n  apiName: %s\nitems:\n"
				if err = writeFinalFile(p, finalFile, header, kind, gv, name, "", gz); err != nil {
					return err
				}
			case JSON:
				header := `{"kind":"%s","apiVersion":"%s","metadata":{"apiName":"%s"},"items":[`
				if err = writeFinalFile(p, finalFile, header, kind, gv, name, "]}", gz); err != nil {
					return err
				}
			case JSON_PRETTY:
				header := "{\n  \"kind\": \"%s\",\n  \"apiVersion\": \"%s\",\n  \"metadata\": {\n    \"apiName\": \"%s\"\n  },\n  \"items\": [\n"
				if err = writeFinalFile(p, finalFile, header, kind, gv, name, "\n  ]\n}", gz); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func writeFinalFile(chunkFile string, finalFile string, header string, kind string, gv string, name string, tail string, gz bool) error {
	f, err := os.OpenFile(finalFile, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	defer os.Remove(chunkFile)
	if _, err = f.WriteString(fmt.Sprintf(header, kind, gv, name)); err != nil {
		return err
	}
	if err = copyFile(chunkFile, f); err != nil {
		return err
	}
	if _, err = f.WriteString(tail); err != nil {
		return err
	}
	if gz {
		if err = gzip(finalFile); err != nil {
			return err
		}
	}
	return nil
}

func getApiListHeaderFromFileName(file string) (string, string, string) {
	h := strings.Split(filepath.Base(file), ".")
	k := h[0]
	n := h[1]
	_gv := strings.Split(h[2], "_")
	gv := _gv[0]
	if len(_gv) > 1 {
		gv = strings.Join(_gv[0:len(_gv)-1], ".") + "/" + _gv[len(_gv)-1]
	}
	return k, n, gv
}

func writeTempChunk(path string, content string, separator string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if stat.Size() > 0 {
		content = separator + content
	}
	if _, err = f.WriteString(content); err != nil {
		return err
	}
	return nil
}

func cleanApiResourcesChunk(apiResources string, name string, gv string, nsExclusionList []string) (string, error) {
	if len(apiResources) == 0 {
		return "", nil
	}
	var (
		cleanApiResource = apiResources
		err              error
	)
	if nsExclusionList != nil {
		if cleanApiResource, err = FilterNS(apiResources, nsExclusionList, false); err != nil {
			return "", err
		}
	}
	if totItems, err := yjq.Eval2Int(yjq.YqEval, ".items | length", cleanApiResource); totItems == 0 {
		e := ""
		if err != nil {
			e = " error converting to integer: " + err.Error()
		}
		logger.Debug(name + " " + gv + " api resource list with zero items" + e)
		return "", nil
	}
	if cleanApiResource, err = yjq.YqEval(DefaultCleaningQuery, cleanApiResource); err != nil {
		return "", err
	}
	if name == "secrets" {
		cleanApiResource, err = yjq.YqEval(`.items[].data.[] = ""`, cleanApiResource)
		if err != nil {
			return "", err
		}
	}
	if name == "packagemanifests" && gv == "packages.operators.coreos.com/v1" {
		cleanApiResource, err = yjq.YqEval(`with(.items[].status.channels[].currentCSVDesc; del(.description) | del (.annotations.alm-examples) | del(.annotations."operatorframework.io/initialization-resource") | del(.annotations."operatorframework.io/suggested-namespace-template"))`, cleanApiResource)
		if err != nil {
			return "", err
		}
		// logger.Info("packagemanifests cleaned")
	}
	if name == "configmaps" {
		cleanApiResource, err = yjq.YqEval(`del(.items[].metadata.annotations."kubernetes.io/description")`, cleanApiResource)
		if err != nil {
			return "", err
		}
	}
	return cleanApiResource, nil
}

func writeLogs(kc Kc, path string, apis string, baseName string, gv string, gz bool, splitns bool, logLine string) error {
	podContainerList, err := yjq.Eval2List(yjq.YqEval, `.items[] | .metadata.name + ";" + .spec.containers[].name + ";" + .metadata.namespace`, apis)
	if err != nil {
		// return writeResourceListLog("podContainerList "+logLine, err)
		return err
	}
	for _, p := range podContainerList {
		_p := strings.Split(p, ";")
		podName := _p[0]
		containerName := _p[1]
		ns := _p[2]
		fileName := podName + "." + containerName + ".log"
		qp := map[string]string{"container": containerName}
		logApi := fmt.Sprintf("%s%s/namespaces/%s/pods/%s/log", baseName, gv, ns, podName)
		if !splitns {
			fileName = ns + "." + fileName
		}
		log, err := kc.
			SetResponseTransformer(apiIgnoreNotFoundResponseTransformer).
			SetGetParams(qp).
			Get(logApi)
		if err != nil {
			if strings.Contains(err.Error(), "container") &&
				strings.Contains(err.Error(), "terminated") {
				return nil
			}
			// return writeResourceListLog("get pod log "+logLine, err)
			return err
		}
		if len(log) > 0 {
			if err = fsutil.WriteTextFile(path+fileName, log); err != nil {
				// return writeResourceListLog("writing pod log "+logLine, err)
				return err
			}
			if gz {
				if err = gzip(path + fileName); err != nil {
					// return writeResourceListLog("gzipping pod log "+logLine, err)
					return err
				}
			}
		}
	}
	return nil
}

func apiIgnoreNotFoundResponseTransformer(kc Kc) (string, error) {
	if kc.Status() == http.StatusNotFound ||
		kc.Status() == http.StatusMethodNotAllowed ||
		kc.Status() == http.StatusGone {
		return "", nil
	}
	return kc.Response(), kc.Err()
}

func writeResourceListLog(msg string, err error) error {
	dumpWorkerErrors.Store(append(dumpWorkerErrors.Load().([]error), fmt.Errorf("%s %w", msg, err)))
	logger.Error("writeResourceList "+msg, zap.Error(err))
	return err
}

func formatResourceContent(contents string, format int, chunked bool) (string, error) {
	var (
		err        error
		newcontent = contents
	)
	// filter wrapped json content in strings
	if slices.Contains([]int{JSON, JSON_LINES, JSON_PRETTY, JSON_LINES_WRAPPED}, format) {
		if newcontent, err = yjq.Y2JC(contents); err != nil {
			return "", err
		}
		newcontent = strings.ReplaceAll(newcontent, `\\\\`, ``)
		newcontent = strings.ReplaceAll(newcontent, `\\`, ``)
		newcontent = strings.ReplaceAll(newcontent, `\n`, ``)
		newcontent = strings.ReplaceAll(newcontent, `\"`, `'`)
		newcontent = strings.ReplaceAll(newcontent, `\t`, ``)
		newcontent = strings.ReplaceAll(newcontent, `\r`, ``)
		newcontent = strings.ReplaceAll(newcontent, `\b`, ``)
		newcontent = strings.ReplaceAll(newcontent, `\f`, ``)
	}
	switch format {
	case YAML:
		if !chunked {
			return newcontent, nil
		}
		return yjq.YqEval("[.items[]]", newcontent)
	case JSON:
		if !chunked {
			return newcontent, nil
		}
		if newcontent, err = yjq.JqEval(".items[]", newcontent); err != nil {
			return "", err
		}
		return strings.ReplaceAll(newcontent, "\n", ","), nil
	case JSON_LINES:
		return yjq.JqEval(".items[]", newcontent)
	case JSON_LINES_WRAPPED:
		return yjq.JqEval(`{"_": .items[]}`, newcontent)
	case JSON_PRETTY:
		if !chunked {
			return yjq.J2JP(newcontent)
		}
		if newcontent, err = yjq.JqEval(".items[]", newcontent); err != nil {
			return "", err
		}
		split := strings.Split(newcontent, "\n")
		var nc strings.Builder
		for i, p := range split {
			pretty, err := yjq.J2JP(p)
			// pretty = pretty[:len(pretty)-1]
			if err != nil {
				return "", err
			}
			if i < len(split)-1 {
				pretty = pretty + ",\n"
			}
			np, err := _sed("s/^/    /", pretty)
			if err != nil {
				return "", err
			}
			nc.WriteString(np[:len(np)-1])
			if i < len(split)-1 {
				nc.WriteString("\n")
			}
		}
		return nc.String(), nil
	default:
		return "", fmt.Errorf("writeTextFile unknown output format requested")
	}
}

func writeResourceFile(path string, contents string, gz bool, format int) error {
	var (
		f          *os.File
		err        error
		newcontent = contents
	)
	if !slices.Contains([]int{YAML, JSON, JSON_LINES, JSON_LINES_WRAPPED, JSON_PRETTY}, format) {
		return fmt.Errorf("unknown format")
	}
	if format != YAML {
		path = strings.Replace(path, ".yaml", ".json", 1)
	}
	if newcontent, err = formatResourceContent(contents, format, false); err != nil {
		return err
	}
	if f, err = os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.WriteString(newcontent); err != nil {
		return err
	}
	if gz {
		return gzip(path)
	}
	return nil
}

func gzip(file string) error {
	originalFile, err := os.Open(file)
	if err != nil {
		return err
	}
	defer originalFile.Close()

	gzippedFile, err := os.Create(file + ".gz")
	if err != nil {
		return err
	}
	defer gzippedFile.Close()

	gzipWriter := gz.NewWriter(gzippedFile)
	defer gzipWriter.Close()

	_, err = io.Copy(gzipWriter, originalFile)
	if err != nil {
		return err
	}

	gzipWriter.Flush()
	os.Remove(file)
	return nil
}

func archive(source string, target string) error {
	var err error

	if _, err = os.Stat(source); err != nil {
		return fmt.Errorf("inexistent tar source: %v", err.Error())
	}

	outFile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer outFile.Close()

	tw := tar.NewWriter(outFile)
	defer tw.Close()

	return filepath.Walk(source, func(file string, fi os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if !fi.Mode().IsRegular() {
			return nil
		}

		if strings.Contains(target, fi.Name()) {
			return nil
		}

		// create a new dir/file header
		header, err := tar.FileInfoHeader(fi, fi.Name())
		if err != nil {
			return err
		}

		// update the name to correctly reflect the desired destination when untaring
		header.Name = strings.TrimPrefix(strings.Replace(file, source, "", -1), string(filepath.Separator))

		// write the header
		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		// open files for taring
		f, err := os.Open(file)
		if err != nil {
			return err
		}

		// copy file data into tar writer
		if _, err := io.Copy(tw, f); err != nil {
			return err
		}

		f.Close()

		return nil
	})
}

func FormatCodeFromString(format string) (int, error) {
	switch format {
	case "yaml":
		return YAML, nil
	case "json":
		return JSON, nil
	case "json_lines":
		return JSON_LINES, nil
	case "json_lines_wrapped":
		return JSON_LINES_WRAPPED, nil
	case "json_pretty":
		return JSON_PRETTY, nil
	default:
		return -1, fmt.Errorf("unknown string coded format %s. please use one of 'yaml', 'json', 'json_pretty', 'json_lines', 'json_lines_wrapped'", format)
	}
}

// TODO: use OR in yq query to avoid loop. not top of the list since filtering is not common
func FilterApiResources(apis string, gvkExclusionList []string) (string, error) {
	var (
		filteredApis = apis
		err          error
	)
	for _, re := range gvkExclusionList {
		r := strings.Split(re, ",")
		if len(r) == 1 {
			r = append(r, ".*")
		}
		if len(r[0]) == 0 {
			r[0] = ".*"
		}
		if len(r[1]) == 0 {
			r[1] = ".*"
		}
		filteredApis, err = yjq.YqEval(`del(.items[] | select(.groupVersion | test("%s") and .kind | test("%s")))`, filteredApis, r[0], r[1])
		if err != nil {
			return apis, err
		}
	}
	return filteredApis, nil
}

// TODO: use OR in yq query to avoid loop. not top of the list since filtering is not common
func FilterNS(apis string, nsExclusionList []string, resourceIsNs bool) (string, error) {
	var (
		selectItem   = ".metadata.namespace"
		filteredApis = apis
		err          error
	)
	if resourceIsNs {
		selectItem = ".metadata.name"
	}
	for _, re := range nsExclusionList {
		filteredApis, err = yjq.YqEval(`del(.items[] | select(%s | test("%s")))`, filteredApis, selectItem, re)
		if err != nil {
			return apis, err
		}
	}
	return filteredApis, nil
}

func copyFile(src string, dst *os.File) error {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()
	_, err = io.Copy(dst, source)
	return err
}

func _sed(expr string, input string) (string, error) {
	r, e := sed.New(strings.NewReader(expr))
	if e != nil {
		return "", e
	}
	s, e := r.RunString(input)
	if e != nil {
		return "", e
	}
	return s, e
}
