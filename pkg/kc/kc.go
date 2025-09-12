package kc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreybutler/go-fsutil"
	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mauricioscastro/kcdump/pkg/util/log"
	"github.com/mauricioscastro/kcdump/pkg/yjq"
)

const (
	Json                     = "json"
	Yaml                     = "yaml"
	TokenPath                = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	CurrentContext           = "current-context"
	queryContextForCluster   = `(%s as $c | .contexts[] | select (.name == $c)).context as $ctx | $ctx | parent | parent | parent | .clusters[] | select(.name == $ctx.cluster) | .cluster.server // ""`
	queryContextForUserAuth  = `(%s as $c | .contexts[] | select (.name == $c)).context as $ctx | $ctx | parent | parent | parent | .users[] | select(.name == $ctx.user) | .user.%s`
	createModifierExtraParam = "?modifier_removed_name="
	currentNamespace         = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
)

var (
	logger                 = log.Logger().Named("kcdump")
	cache                  sync.Map
	overrideAcceptWithJson map[string]string
	overrideAcceptWithYaml map[string]string
	kubeConfigFromStdin    []byte
)

type (
	// Kc represents a kubernetes client
	Kc interface {
		SetToken(token string) Kc
		SetCert(cert tls.Certificate) Kc
		SetCluster(cluster string) Kc
		Get(apiCall string, headers ...map[string]string) (string, error)

		// regular k8s apply for a given manifest over zero or more namespaces.
		// if first namespace item is "*" will apply to all namespaces.
		// if manifest document(s) has '.metadata.namespace' namespaces parameter is ignored
		Apply(yamlManifest string, namespaces ...string) (string, error)
		apply(apiCall string, body string) (string, error)
		applyModifier(apiUrl string, body string, not_used bool) (string, error)

		Create(yamlManifest string, applyIfFound bool, namespaces ...string) (string, error)
		create(apiCall string, body string, applyIfFound bool) (string, error)

		// regular k8s replace for a given manifest over zero or more namespaces.
		// if first namespace item is "*" will replace in all namespaces.
		// if manifest document(s) has '.metadata.namespace' namespaces parameter is ignored
		Replace(yamlManifest string, applyIfNotFound bool, namespaces ...string) (string, error)
		replace(apiCall string, body string, applyIfNotFound bool) (string, error)

		Delete(yamlManifest string, ignoreNotFound bool, namespaces ...string) (string, error)
		delete(apiCall string, ignoreNotFound bool) (string, error)
		deleteModifier(apiUrl string, not_used string, ignoreNotFound bool) (string, error)

		modify(modifier modifier, yamlManifest string, ignoreNotFound bool, isCreating bool, namespaces ...string) (string, error)

		// target format = namespace/pod/container. if container is omitted the first is used.
		// if namespace is omitted, as in "/echo-/" it assumes it's running inside the cluster and will try '/var/run/secrets/kubernetes.io/serviceaccount/namespace
		// pod name can be a substring of the target pod in such case all pods containing such substring in the replica list are used
		// returns stdout, stderr
		Exec(target string, cmd []string) (string, string, error)

		// src (source) and dst (destination) can be file:/absolute_path_to_local_file or
		// pod:/namespace/pod/container:/absolute_path_to_file_in_container
		// if 'pod:/' or 'file:/' as destination have trailing "/" the source file name is used
		// as file name and written into that path assumed to be a directory.
		// copy operations needs the targeted container to have the 'dd' utility in its path. if
		// not in path you can set its absolute path location with SetDDpath().
		// in 'pod:/[...]' the pod name can be a substring of the target pod for which all
		// pods are used if more than one replica exists. container is optional and when this
		// happens the 1st found is used. if pod file destination is not specified while copying to pod '/tmp' directory will be used as target.
		// if namespace is omitted, as in "pod://echo-/" it assumes it's running inside the cluster and will try '/var/run/secrets/kubernetes.io/serviceaccount/namespace
		Copy(src string, dst string) error

		// target format = namespace/pod/container
		// if namespace is omitted, as in "/echo-/" it assumes it's running inside the cluster and will try '/var/run/secrets/kubernetes.io/serviceaccount/namespace
		// pod name can be a substring of the target pod in such case first found pod name containing such substring in the replica list is used
		// if container is omitted logs from all containers are retrieved.
		// tailLines = -1 get all.
		// tailLines > 0 get that number of log lines
		Log(target string, tailLines int) (string, error)

		// 'dd' utility for copy operation if not in container's path. default is "dd"
		// as in (...)/exec?container=db&command=dd(...)&stdout=true&stderr=true&stdin=true"
		SetDDpath(ddAbsolutePath string) Kc
		SetGetParams(queryParams map[string]string) Kc
		SetGetParam(name string, value string) Kc
		Accept(format string) Kc
		SetResponseTransformer(transformer ResponseTransformer) Kc
		Version() string
		Response() string
		Err() error
		StatusCode() int
		Status() string
		Cluster() string
		Api() string
		Ns() (string, error)
		NsNames() ([]string, error)
		ApiResources() (string, error)
		Dump(path string,
			nsExclusionList []string,
			gvkExclusionList []string,
			syncChunkMap map[string]int,
			asyncChunkMap map[string]int,
			gz bool,
			tgz bool,
			prune bool,
			splitns bool,
			splitgv bool,
			format int,
			poolSize int,
			chunkSize int,
			escapeEncodedJson bool,
			copyToPod string,
			filename string,
			tailLines int,
			ignoreWorkerErrors bool,
			progress func()) error
		setCert(cert []byte, key []byte)
		send(method string, apiCall string, body string) (string, error)
		setResourceVersion(apiCall string, newResource string) (string, error)
		writeResourceList(path string, baseName string, name string, gv string, namespaced bool, splitns bool, nsExclusionList []string, nologs bool, gz bool, format int, chunkSize int, escapeEncodedJson bool, tailLines int, progress func()) error
		GetApiResourceNameNamespacedFromGvk(gv string, k string) (string, string, error)
		shallowCopy() Kc
	}

	kc struct {
		client     *resty.Client
		readOnly   bool
		cluster    string
		api        string
		resp       string
		err        error
		statusCode int
		status     string
		accept     string
		// for copy operation
		ddPath      string
		transformer ResponseTransformer
		cert        tls.Certificate
		token       string
	}
	// Optional transformer function to Get methods
	// should return ('transformed response', 'transformed error')
	ResponseTransformer func(Kc) (string, error)

	// apiUrl, body, ignore
	modifier func(string, string, bool) (string, error)

	cacheEntry struct {
		version              string
		gvkMapNameNamespaced map[string]string // "gv:k" is key, "name:namespaced" is value
	}
)

func init() {
	cache = sync.Map{}
	overrideAcceptWithJson = map[string]string{"Accept": "application/" + Json}
	overrideAcceptWithYaml = map[string]string{"Accept": "application/" + Yaml}
	kubeConfigFromStdin = make([]byte, 0)
}

func SetLogLevel(level string) {
	log.ResetLoggerLevel(logger, level)
}

func NewKc() Kc {
	token, err := os.ReadFile(TokenPath)
	if err != nil {
		logger.Debug("could not read default sa token. skipping to kubeconfig", zap.Error(err))
	} else {
		logger.Debug("", zap.String("token from sa account", "x-x-x-x-x"))
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
		if host != "" && port != "" {
			host = "https://" + host
			logger.Debug("kc will auth with token and env KUBERNETES_SERVICE_...")
			logger.Debug("", zap.String("host:port from env", host+":"+port))
			return NewKcWithToken(host+":"+port, string(token))
		}
	}
	return NewKcWithContext(CurrentContext)
}

func NewKcWithContext(context string) Kc {
	home, err := os.UserHomeDir()
	if err != nil {
		logger.Error("reading home info", zap.Error(err))
		return newKc()
	}
	return NewKcWithConfigContext(filepath.FromSlash(home+"/.kube/config"), context)
}

func NewKcWithConfig(config string) Kc {
	return NewKcWithConfigContext(config, CurrentContext)
}

func NewKcWithConfigContext(config string, context string) Kc {
	logger.Debug("context " + context)
	logger.Debug("config=" + config)
	kcfg, err := os.ReadFile(config)
	if err != nil {
		if len(kubeConfigFromStdin) == 0 {
			logger.Error("reading config file. will try reading from stdin...", zap.Error(err))
			kubeConfigFromStdin, err = io.ReadAll(os.Stdin)
			if err != nil {
				logger.Error("reading config from stdin also failed.", zap.Error(err))
				return nil
			}
		}
		logger.Debug("using last config read from stdin")
		kcfg = kubeConfigFromStdin
	}
	kubeCfg := string(kcfg)
	if context == CurrentContext {
		context, err = yjq.YqEval(fmt.Sprintf(`.%s // ""`, CurrentContext), kubeCfg)
		if err != nil || context == "" {
			logger.Error("reading context info", zap.Error(err))
			return nil
		}
	}
	context = `"` + context + `"`
	cluster, err := yjq.YqEval(fmt.Sprintf(queryContextForCluster, context), kubeCfg)
	logger.Debug("query for server " + fmt.Sprintf(queryContextForCluster, context))
	if err != nil {
		logger.Error("reading cluster info for context", zap.Error(err))
		return nil
	}
	if len(cluster) == 0 {
		logger.Error("empty cluster info reading context")
		return nil
	}
	logger.Debug("", zap.String("context cluster", cluster))
	kc := newKc()
	kc.SetCluster(cluster)
	token, err := yjq.YqEval(fmt.Sprintf(queryContextForUserAuth, context, `token // ""`), kubeCfg)
	if err != nil {
		logger.Error("reading token info for context", zap.Error(err))
		return nil
	}
	if len(token) == 0 {
		logger.Debug("empty user token info reading context. trying user cert...")
		cert, err := yjq.YqEval(fmt.Sprintf(queryContextForUserAuth, context, `client-certificate-data // "" | @base64d`), kubeCfg)
		if err != nil {
			logger.Error("reading user cert info for context", zap.Error(err))
			return nil
		}
		if len(cert) == 0 {
			// is it in a file?
			cert, err = yjq.YqEval(fmt.Sprintf(queryContextForUserAuth, context, `client-certificate // ""`), kubeCfg)
			if err != nil || len(cert) == 0 {
				logger.Error("reading user cert info for context or empty cert file", zap.Error(err))
				return nil
			}
			cert, err = fsutil.ReadTextFile(cert)
			if err != nil || len(cert) == 0 {
				logger.Error("reading user cert from file or empty cert file", zap.Error(err))
				return nil
			}
		}
		logger.Debug("", zap.String("cert", cert))
		key, err := yjq.YqEval(fmt.Sprintf(queryContextForUserAuth, context, `client-key-data // "" | @base64d`), kubeCfg)
		if err != nil {
			logger.Error("reading user cert key info for context", zap.Error(err))
			return nil
		}
		if len(key) == 0 {
			// is it in a file?
			key, err = yjq.YqEval(fmt.Sprintf(queryContextForUserAuth, context, `client-key // ""`), kubeCfg)
			if err != nil || len(key) == 0 {
				logger.Error("reading user cert key info for context or empty key file", zap.Error(err))
				return nil
			}
			key, err = fsutil.ReadTextFile(key)
			if err != nil || len(key) == 0 {
				logger.Error("reading user cert key from file or empty key file", zap.Error(err))
				return nil
			}
		}
		logger.Debug("", zap.String("key", "x-x-x-x-x"))
		kc.setCert([]byte(cert), []byte(key))
	} else {
		logger.Debug("", zap.String("token from kube config", "x-x-x-x-x"))
		kc.SetToken(token)
	}
	logger.Debug("kc will auth with kube config")
	return kc
}

func NewKcWithCert(cluster string, cert []byte, key []byte) Kc {
	kc := newKc().SetCluster(cluster)
	kc.setCert(cert, key)
	return kc
}

func NewKcWithToken(cluster string, token string) Kc {
	return newKc().SetCluster(cluster).SetToken(token)
}

func (kc *kc) setCert(cert []byte, key []byte) {
	c, err := tls.X509KeyPair(cert, key)
	if err != nil {
		logger.Error("cert creation failure", zap.Error(err))
	} else {
		kc.SetCert(c)
	}
}

func newKc() Kc {
	kc := kc{}
	kc.client = resty.New().
		SetTLSClientConfig(&tls.Config{InsecureSkipVerify: true}).
		SetTimeout(5*time.Minute).
		SetRetryCount(5).
		SetRetryWaitTime(500*time.Millisecond).
		SetHeader("User-Agent", "kc/v.0.0.0")
	kc.readOnly = false
	kc.accept = "application/yaml"
	kc.ddPath = "dd" // for copy operation
	yjq.SilenceYqLogs()
	return &kc
}

func (kc *kc) shallowCopy() Kc {
	if len(kc.cert.Certificate) > 0 {
		return newKc().SetCluster(kc.cluster).SetCert(kc.cert)
	}
	if kc.token != "" {
		return newKc().SetCluster(kc.cluster).SetToken(kc.token)
	}
	return nil
}

func (kc *kc) SetCluster(cluster string) Kc {
	kc.cluster = cluster
	kc.client.SetBaseURL(kc.cluster)
	cache.Store(kc.cluster, cacheEntry{"", make(map[string]string)})
	return kc
}

func (kc *kc) Cluster() string {
	return kc.cluster
}

func (kc *kc) SetToken(token string) Kc {
	kc.token = token
	kc.client.SetAuthToken(token)
	return kc
}

func (kc *kc) SetCert(cert tls.Certificate) Kc {
	kc.cert = cert
	kc.client.SetCertificates(cert)
	return kc
}

func (kc *kc) SetDDpath(ddAbsolutePath string) Kc {
	kc.ddPath = url.QueryEscape(ddAbsolutePath)
	return kc
}

func (kc *kc) Accept(format string) Kc {
	if format != Json && format != Yaml {
		logger.Error("wrong accept format " + format + ". will use yaml")
		format = Yaml
	}
	kc.accept = "application/" + format
	return kc
}

func (kc *kc) SetResponseTransformer(transformer ResponseTransformer) Kc {
	kc.transformer = transformer
	return kc
}

func (kc *kc) Response() string {
	return kc.resp
}

func (kc *kc) Err() error {
	return kc.err
}

func (kc *kc) StatusCode() int {
	return kc.statusCode
}

func (kc *kc) Status() string {
	return kc.status
}

func (kc *kc) Api() string {
	return kc.api
}

func getCopyParams(kc *kc, src string, dst string) (bool, string, string, string, map[string][]string, error) {
	pod_container := make(map[string][]string)
	src = strings.Trim(src, " ")
	dst = strings.Trim(dst, " ")
	logger.Debug("src=" + src + " dst=" + dst)
	if (!strings.HasPrefix(src, "file:/") && !strings.HasPrefix(src, "pod:/")) ||
		(!strings.HasPrefix(dst, "file:/") && !strings.HasPrefix(dst, "pod:/")) {
		return false, "", "", "", pod_container, errors.New("source and or destiny does not follow 'file:/' 'pod:/' pattern")
	}
	if (strings.HasPrefix(src, "file:/") && strings.HasPrefix(dst, "file:/")) ||
		(strings.HasPrefix(src, "pod:/") && strings.HasPrefix(dst, "pod:/")) {
		return false, "", "", "", pod_container, errors.New("source and destiny cannot be both 'file:/' or both 'pod:/'")
	}
	sending := strings.HasPrefix(src, "file:/")
	localFile, podFile, namespace, pod, container := "", "", "", "", ""
	local := src
	remote := dst
	if !sending {
		local = dst
		remote = src
	}
	remote = remote[5:]
	localFile = local[6:]
	logger.Debug("localfile=" + localFile)
	if sending && !fsutil.IsFile(localFile) {
		return false, "", "", "", pod_container, errors.New("pattern 'file:/' in copyTo used with non file (directory?) or inexistent")
	}
	namespace, pod, container, path, err := getPodTarget(remote)
	if err != nil {
		return false, "", "", "", pod_container, err
	}
	logger.Debug("namespace=" + namespace + " pod=" + pod + " container=" + container + " path=" + path)
	if path == "" {
		if !sending {
			return false, "", "", "", pod_container, errors.New("getCopyParams: absent pod source file in pod:/" + remote)
		}
		logger.Info("no directory specified, copying to /tmp")
		podFile = "/tmp/" + filepath.Base(localFile)
	} else {
		if strings.HasSuffix(path, "/") {
			if !sending {
				return false, "", "", "", pod_container, errors.New("getCopyParams: absent pod source file in pod:/" + remote)
			}
			podFile = path + filepath.Base(localFile)
		} else {
			podFile = path
		}
	}
	logger.Sugar().Debug("getCopyParams: sending=", sending, " localfile=", localFile, " podfile=", podFile)
	if !sending && strings.HasSuffix(localFile, "/") {
		localFile = localFile + filepath.Base(podFile)
	}
	pod_container = getPodAndContainerMap(kc, namespace, pod, container)
	logger.Debug("getPodAndContainer ----> namespace=" + namespace + " pod_container=" + fmt.Sprint(pod_container))
	return sending, localFile, podFile, namespace, pod_container, nil
}

func (kc *kc) Copy(src string, dst string) error {
	sending, localFile, podFile, namespace, pod_container, err := getCopyParams(kc, src, dst)
	if err != nil {
		kc.err = err
		logger.Error(err.Error())
		return err
	}
	pod_container_k := slices.Collect(maps.Keys(pod_container))
	if len(pod_container_k) == 0 {
		err = fmt.Errorf("Copy: not found source=%s target=%s", src, dst)
		kc.err = err
		return err
	}
	for pod, container_list := range pod_container {
		container := container_list[0]
		stdin := false
		fileSizeReportedToDD := int64(-1)
		queryCmd := fmt.Sprintf("&command=%s&command=%s&command=%s", kc.ddPath, url.QueryEscape("bs=1"), url.QueryEscape("if="+podFile))
		if sending {
			stdin = true
			info, err := os.Stat(localFile)
			if err != nil {
				kc.err = err
				logger.Error(err.Error())
				return err
			}
			fileSizeReportedToDD = info.Size()
			queryCmd = fmt.Sprintf("&command=%s&command=%s&command=%s&command=%s%d", kc.ddPath, url.QueryEscape("of="+podFile), url.QueryEscape("bs=1"), url.QueryEscape("count="), fileSizeReportedToDD)
		}
		logger.Debug("copy cmd", zap.Bool("isSending", sending), zap.String("reqCmd", queryCmd))
		wsConn, resp, err := getWsConn(kc, namespace, pod, container, queryCmd, stdin)
		if err != nil {
			kc.err = err
			logger.Error("copy dial ws conn:", zap.Error(err))
			logger.Error("check namespace, pod and container", zap.String("namespace", namespace), zap.String("pod", pod), zap.String("container", container))
			return err
		}
		for k, v := range resp.Header {
			logger.Debug("copy resp headers", zap.Strings(k, v))
		}
		logger.Debug("Copy", zap.String("src", src), zap.String("dst", dst))
		if sending {
			err = copyTo(wsConn, localFile, fileSizeReportedToDD)
		} else {
			err = copyFrom(wsConn, localFile, src)
		}
		if err != nil {
			kc.err = err
			logger.Error("Copy:", zap.Error(err))
			logger.Error("check namespace, pod and container", zap.String("namespace", namespace), zap.String("pod", pod), zap.String("container", container))
		}
		wsConn.Close()
	}
	return nil
}

func copyFrom(wsConn *websocket.Conn, localFile string, src string) error {
	var stdErr strings.Builder
	writer, err := os.Create(localFile)
	if err != nil {
		logger.Error("ws dial copyFrom open file:" + err.Error())
		return err
	}
	defer writer.Close()
	for {
		_, reader, err := wsConn.NextReader()
		if err != nil {
			ce, ok := err.(*websocket.CloseError)
			if !ok || ce.Code != websocket.CloseNormalClosure {
				logger.Error("ws dial copyFrom nextReader:" + err.Error())
				return err
			}
			break
		}
		b := make([]byte, 4096)
		for {
			n, err := reader.Read(b)
			// logger.Sugar().Debug("received ", n, "bytes")
			if n > 0 {
				if b[0] == 1 && len(b) > 1 {
					// stdout, send to local file
					_, err = writer.Write(b[1:n])
					if err != nil {
						logger.Error("ws dial copyFrom writing to local file:" + err.Error())
						break
					}
				}
				if b[0] == 2 && len(b) > 1 {
					// stderr, collect
					_, err = stdErr.Write(b[1:n])
					if err != nil {
						logger.Error("ws dial copyFrom writing to stderr buffer:" + err.Error())
						break
					}
				}
			}
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
		}
	}
	info, err := os.Stat(localFile)
	if err != nil {
		return fmt.Errorf("file stat copyFrom. localfile=%s", localFile)
	}
	logger.Sugar().Debug(" dd stderr=", stdErr.String())
	logger.Sugar().Debug("local size=", info.Size())

	pattern := fmt.Sprintf(`(?s)%d.*in.*%d.*out`, info.Size(), info.Size())
	match, err := regexp.MatchString(pattern, stdErr.String())
	if err != nil {
		return err
	}
	if !match {
		return fmt.Errorf("%s file size = %d. different from dd = %s", localFile, info.Size(), stdErr.String())
	}
	return nil
}

func copyTo(wsConn *websocket.Conn, localFile string, fileSizeReportedToDD int64) error {
	logger.Sugar().Debug("sending file "+localFile+" with reported size ", fileSizeReportedToDD)
	reader, err := os.Open(localFile)
	if err != nil {
		return err
	}
	defer reader.Close()
	b := make([]byte, 4096)
	b[0] = 0
	read := int64(0)
	for {
		r, err := reader.Read(b[1:])
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Debug("copyTo. read from file " + err.Error())
			return err
		}
		read += int64(r)
		err = wsConn.WriteMessage(websocket.BinaryMessage, b[:r+1])
		if err != nil {
			logger.Debug("copyTo. wsConn.WriteMessage " + err.Error())
			return err
		}
	}
	logger.Sugar().Debug("read=", read)
	if read != fileSizeReportedToDD {
		return fmt.Errorf("number of bytes sent to dd is less than expected")
	}
	var stdErr strings.Builder
	for {
		_, m, e := wsConn.ReadMessage()
		if e != nil {
			ce, ok := e.(*websocket.CloseError)
			if !ok || ce.Code != websocket.CloseNormalClosure {
				logger.Error("ws dial copyTo read message: " + e.Error())
			}
			break
		}
		stdErr.WriteString(string(m[1:]))
	}
	info, err := os.Stat(localFile)
	if err != nil {
		return fmt.Errorf("file stat copyFrom. localfile=%s", localFile)
	}
	logger.Sugar().Debug(" dd stderr=", stdErr.String())
	logger.Sugar().Debug("local size=", info.Size())
	pattern := fmt.Sprintf(`(?s)%d.*in.*%d.*out`, info.Size(), info.Size())
	match, err := regexp.MatchString(pattern, stdErr.String())
	if err != nil {
		return err
	}
	if !match {
		return fmt.Errorf("%s file size = %d. different from dd = %s", localFile, info.Size(), stdErr.String())
	}
	return nil
}

func (kc *kc) Log(target string, tailLines int) (string, error) {
	if tailLines == 0 {
		return "", nil
	}
	namespace, pod, container, _, err := getPodTarget(target)
	if err != nil {
		return "", err
	}
	pod_container := getPodAndContainerMap(kc, namespace, pod, container)
	logger.Debug("getPodAndContainer ----> namespace=" + namespace + " pod_container=" + fmt.Sprint(pod_container))
	pod_container_k := slices.Collect(maps.Keys(pod_container))
	if len(pod_container_k) == 0 {
		return "", fmt.Errorf("getCopyParams: not found namespace=%s pod=%s container=%s", namespace, pod, container)
	}
	var log strings.Builder
	for pod, container_list := range pod_container {
		logger.Debug("Log", zap.String("pod", pod), zap.String("namespace", namespace))
		for _, container := range container_list {
			logger.Debug("Log", zap.String("namespace", namespace), zap.String("pod", pod), zap.String("container", container))
			qp := map[string]string{"container": container}
			if tailLines > 0 {
				qp["tailLines"] = fmt.Sprintf("%d", tailLines)
			}
			logApi := fmt.Sprintf("/api/%s/namespaces/%s/pods/%s/log", kc.Version(), namespace, pod)
			logger.Debug("will get the logs for pod " + pod + " container " + container)
			l, err := kc.
				SetResponseTransformer(apiIgnoreNotFoundResponseTransformer).
				SetGetParams(qp).
				Get(logApi)
			if err != nil {
				if !(strings.Contains(err.Error(), "container") &&
					strings.Contains(err.Error(), "terminated")) {
					logger.Error("", zap.Error(err))
				}
				continue
			}
			if len(l) == 0 {
				continue
			}
			if len(container_list) > 1 {
				log.WriteString("===> " + namespace + "/" + pod + "/" + container + " <===\n" + l)
			} else {
				log.WriteString(l)
			}
		}
	}
	return log.String(), nil
}

func (kc *kc) Exec(target string, cmd []string) (string, string, error) {
	namespace, pod, container, _, err := getPodTarget(target)
	if err != nil {
		return "", "", err
	}
	pod_container := getPodAndContainerMap(kc, namespace, pod, container)
	logger.Debug("getPodAndContainer ----> namespace=" + namespace + " pod_container=" + fmt.Sprint(pod_container))
	pod_container_k := slices.Collect(maps.Keys(pod_container))
	if len(pod_container_k) == 0 {
		return "", "", fmt.Errorf("getCopyParams: not found namespace=%s pod=%s container=%s", namespace, pod, container)
	}
	var stdOut, stdErr strings.Builder
	for _pod, container_list := range pod_container {
		_container := container_list[0]
		logger.Info("exec in pod " + _pod + " container " + _container)
		queryCmd := ""
		for _, c := range cmd {
			queryCmd = queryCmd + "&command=" + url.QueryEscape(c)
		}
		wsConn, resp, err := getWsConn(kc, namespace, _pod, _container, queryCmd, false)
		if err != nil {
			logger.Error("exec dial ws conn:", zap.Error(err))
			logger.Error("check namespace, pod and container", zap.String("namespace", namespace), zap.String("pod", pod), zap.String("container", container))
			return "", "", err
		}
		for k, v := range resp.Header {
			logger.Debug("exec resp headers", zap.Strings(k, v))
		}
		if len(pod_container_k) > 1 {
			h := "===> " + namespace + "/" + _pod + "/" + _container + " <===\n"
			stdOut.WriteString(h)
			stdErr.WriteString(h)
		}
		for {
			_, m, e := wsConn.ReadMessage()
			if e != nil {
				ce, ok := e.(*websocket.CloseError)
				if !ok || ce.Code != websocket.CloseNormalClosure {
					logger.Error("ws read:" + e.Error())
				}
				break
			}
			if m[0] == 1 {
				stdOut.WriteString(string(m[1:]))
			}
			if m[0] == 2 {
				stdErr.WriteString(string(m[1:]))
			}
		}
		wsConn.Close()
	}
	return stdOut.String(), stdErr.String(), nil
}

func getWsConn(kc *kc, namespace string, _pod string, _container string, queryCmd string, stdin bool) (*websocket.Conn, *http.Response, error) {
	var header http.Header = nil
	if kc.client.Token != "" {
		header = make(http.Header)
		header["Authorization"] = []string{"Bearer " + kc.client.Token}
	}
	dialer := websocket.DefaultDialer
	dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	if kc.cert.PrivateKey != nil {
		dialer.TLSClientConfig.Certificates = append(dialer.TLSClientConfig.Certificates, kc.cert)
	}
	server := strings.Replace(kc.cluster, "https://", "", -1)
	wsUrl := "wss://%s/api/%s/namespaces/%s/pods/%s/exec?container=%s%s&stdout=true&stderr=true&stdin=%t"
	wsUrl = fmt.Sprintf(wsUrl, server, kc.Version(), namespace, _pod, _container, queryCmd, stdin)
	logger.Debug("ws url", zap.String("wsUrl", wsUrl))
	return dialer.Dial(wsUrl, header)
}

func getPodTarget(target string) (string, string, string, string, error) {
	_target, path := target, ""
	if i := strings.Index(target, ":"); i != -1 {
		_target = target[0:i]
		if i == len(target)-1 {
			path = ""
		} else {
			path = target[i+1:]
		}
	}
	logger.Debug("getPodTarget", zap.String("_target", _target), zap.String("path", path))
	targetList := strings.Split(_target, "/")
	if len(targetList) < 2 {
		return "", "", "", "", fmt.Errorf("wrong target format %s. should be '[namespace]/pod[/container[:/path]]'", target)
	}
	if len(targetList[0]) == 0 {
		logger.Debug("getPodTarget with no namespace")
		ns, e := fsutil.ReadTextFile(currentNamespace)
		if e != nil {
			return "", "", "", "", e
		}
		targetList[0] = ns
	}
	namespace := targetList[0]
	pod := targetList[1]
	container := ""
	if len(targetList) > 2 {
		container = targetList[2]
	}
	logger.Debug("getPodTarget", zap.String("namespace", namespace), zap.String("pod", pod), zap.String("container", container), zap.String("path", path))
	return namespace, pod, container, path, nil
}

func getPodAndContainerMap(kc *kc, ns string, podSubstring string, container string) map[string][]string {
	pod_container := make(map[string][]string)
	pods, e := kc.Get("/api/"+kc.Version()+"/namespaces/"+ns+"/pods", overrideAcceptWithYaml)
	if e != nil {
		logger.Error("", zap.Error(e))
		return pod_container
	}
	list, e := yjq.Eval2List(yjq.YqEval, `.items[] | .metadata.name + "," + ([.spec.containers[].name] | join(";"))`, pods)
	if e != nil {
		logger.Error("", zap.Error(e))
		return pod_container
	}
	for _, p := range list {
		item := strings.Split(p, ",")
		if strings.Contains(item[0], podSubstring) {
			if container != "" {
				pod_container[item[0]] = []string{container}
			} else {
				pod_container[item[0]] = strings.Split(item[1], ";")
			}
		}
	}
	return pod_container
}

func (kc *kc) Get(apiCall string, headers ...map[string]string) (string, error) {
	kc.api = apiCall
	kc.resp = ""
	kc.err = nil
	rc := kc.client.SetHeader("Accept", kc.accept).R()
	for _, h := range headers {
		rc.SetHeaders(h)
	}
	resp, err := rc.Get(apiCall)
	if err != nil {
		return "", err
	}
	logResponse(apiCall, resp)
	kc.statusCode = resp.StatusCode()
	kc.status = resp.Status()
	kc.resp = string(resp.Body())
	if kc.statusCode >= 400 {
		kc.err = errors.New(resp.Status() + "\n" + kc.resp)
	}
	if kc.transformer != nil {
		kc.resp, kc.err = kc.transformer(kc)
	}
	return kc.resp, kc.err
}

func (kc *kc) apply(apiCall string, body string) (string, error) {
	return kc.applyModifier(apiCall, body, false)
}

func (kc *kc) Apply(yamlManifest string, namespaces ...string) (string, error) {
	return kc.modify(kc.applyModifier, yamlManifest, false, false, namespaces...)
}

func (kc *kc) applyModifier(apiUrl string, body string, not_used bool) (string, error) {
	logger.Debug("apply", zap.String("apiCall", apiUrl))
	return kc.send(http.MethodPatch, apiUrl, body)
}

func (kc *kc) create(apiCall string, body string, applyIfFound bool) (string, error) {
	logger.Debug("create", zap.String("apiCall", apiCall))
	if applyIfFound {
		// need to re-add the name to the url if it comes from modifier
		_apiCall := strings.Replace(apiCall, createModifierExtraParam, "/", 1)
		_, e := kc.Get(_apiCall)
		if e == nil {
			logger.Sugar().Infof("tried to create already existing resource %s. will apply instead", apiCall)
			return kc.send(http.MethodPatch, _apiCall, body)
		}
	}
	// remove extra param from url if it comes from modifier
	if strings.Contains(apiCall, createModifierExtraParam) {
		apiCall = apiCall[0:strings.Index(apiCall, createModifierExtraParam)]
		logger.Debug("create new apiCall", zap.String("apiCall", apiCall))
	}
	return kc.send(http.MethodPost, apiCall, body)
}

func (kc *kc) Create(yamlManifest string, applyIfFound bool, namespaces ...string) (string, error) {
	return kc.modify(kc.create, yamlManifest, applyIfFound, true, namespaces...)
}

func (kc *kc) Replace(yamlManifest string, applyIfNotFound bool, namespaces ...string) (string, error) {
	return kc.modify(kc.replace, yamlManifest, applyIfNotFound, false, namespaces...)
}

func (kc *kc) replace(apiCall string, body string, applyIfNotFound bool) (string, error) {
	logger.Debug("replace", zap.String("apiCall", apiCall))
	if applyIfNotFound {
		_, e := kc.Get(apiCall)
		if e != nil && kc.statusCode == 404 {
			logger.Sugar().Infof("tried to replace non existent resource %s. will apply instead", apiCall)
			return kc.send(http.MethodPatch, apiCall, body)
		}
	}
	return kc.send(http.MethodPut, apiCall, body)
}

func (kc *kc) delete(apiCall string, ignoreNotFound bool) (string, error) {
	return kc.deleteModifier(apiCall, "", ignoreNotFound)
}

func (kc *kc) Delete(yamlManifest string, ignoreNotFound bool, namespaces ...string) (string, error) {
	return kc.modify(kc.deleteModifier, yamlManifest, ignoreNotFound, false, namespaces...)
}

func (kc *kc) deleteModifier(apiCall string, not_used string, ignoreNotFound bool) (string, error) {
	logger.Debug("delete", zap.String("apiCall", apiCall))
	kc.api = apiCall
	kc.resp = ""
	kc.err = nil
	resp, err := kc.client.SetHeader("Accept", kc.accept).R().Delete(apiCall)
	if err != nil {
		return "", err
	}
	logResponse(apiCall, resp)
	if resp.StatusCode() == http.StatusNotFound && ignoreNotFound {
		return "", nil
	}
	kc.resp = string(resp.Body())
	if resp.StatusCode() >= 400 {
		return "", errors.New(resp.Status() + "\n" + kc.resp)
	}
	kc.status = resp.Status()
	kc.statusCode = resp.StatusCode()
	return kc.resp, nil
}

func (kc *kc) modify(modifier modifier, yamlManifest string, ignore bool, isCreating bool, namespaces ...string) (string, error) {
	if kc.readOnly {
		return "", errors.New("trying to modify resources in read only mode")
	}
	var errList, respList strings.Builder
	for docIndex := 0; ; docIndex++ {
		yamlDoc, err := yjq.YqEval("select(di==%d)", yamlManifest, docIndex)
		if err != nil {
			respList, errList = updateRespLists("", err, errList, respList, "selecting doc from manifest", docIndex > 0)
			break
		}
		if yamlDoc == "" {
			break
		}
		gv, k, ns, name, err := getGvkNsNameFromYamlDoc(yamlDoc)
		if err != nil {
			respList, errList = updateRespLists("", err, errList, respList, "retrieving gv, k and name from doc", docIndex > 0)
			continue
		}
		apiCall := "/api/"
		if gv != kc.Version() {
			apiCall = "/apis/"
		}
		apirsname, nsed, err := kc.GetApiResourceNameNamespacedFromGvk(gv, k)
		if err != nil {
			respList, errList = updateRespLists("", err, errList, respList, "retrieving api resource name from gvk="+gv+":"+k, docIndex > 0)
			continue
		}
		namespaced, err := strconv.ParseBool(nsed)
		if err != nil {
			respList, errList = updateRespLists("", err, errList, respList, "parsing bool for namespaced value", docIndex > 0)
			continue
		}
		if !namespaced {
			apiCall = apiCall + gv + "/" + apirsname
			if isCreating {
				apiCall = apiCall + createModifierExtraParam + name
			} else {
				apiCall = apiCall + "/" + name
			}
			logger.Debug("modifying non namespaced url " + apiCall)
			r, e := modifier(apiCall, yamlDoc, ignore)
			respList, errList = updateRespLists(r, e, errList, respList, "non namespaced api "+apiCall, docIndex > 0)
		} else {
			urls, err := getApiUrlListForNs(kc, apiCall, gv, apirsname, ns, namespaces)
			if err != nil {
				return "", err
			}
			for i, apiUrl := range urls {
				if isCreating {
					apiUrl = apiUrl + createModifierExtraParam + name
				} else {
					apiUrl = apiUrl + "/" + name
				}
				logger.Debug("modifying namespaced url " + apiUrl)
				resp, err := modifier(apiUrl, yamlDoc, ignore)
				respList, errList = updateRespLists(resp, err, errList, respList, "namespaced url "+apiUrl, (i > 0 || docIndex > 0))
			}
		}
	}
	logger.Debug("errList=" + errList.String() + " respList=" + respList.String())
	kc.resp, kc.err = respList.String(), errors.New(errList.String())
	return kc.resp, kc.err
}

func updateRespLists(resp string, err error, errList strings.Builder, respList strings.Builder, msg string, appendNl bool) (strings.Builder, strings.Builder) {
	if err != nil {
		if appendNl {
			errList.WriteString("\n")
		}
		errList.WriteString(fmt.Sprintf("%s: %s", msg, err.Error()))
	} else {
		if appendNl {
			respList.WriteString("\n")
		}
		respList.WriteString(resp)
	}
	return respList, errList
}

func getApiUrlListForNs(kc *kc, apiCall string, gv string, apiResourceName string, nsFromDoc string, namespaces []string) ([]string, error) {
	if nsFromDoc != "" {
		if len(namespaces) != 0 {
			logger.Warn("namespace specified in document has precedence and extra namespaces given will be ignored", zap.Strings("namespaces", namespaces))
		}
		namespaces = []string{nsFromDoc}
	} else {
		if len(namespaces) == 0 {
			ns, e := fsutil.ReadTextFile(currentNamespace)
			if e != nil {
				return []string{}, errors.New("no namespace specified and current namespace could not be read from " + currentNamespace)
			}
			namespaces = []string{ns}
		} else if namespaces[0] == "*" {
			all, err := kc.NsNames()
			if err != nil {
				return []string{}, err
			}
			namespaces = all
		}
	}
	apiCallList := []string{}
	for _, nsname := range namespaces {
		apiCallList = append(apiCallList, apiCall+gv+"/namespaces/"+nsname+"/"+apiResourceName)
	}
	return apiCallList, nil
}

func (kc *kc) Version() string {
	c, _ := cache.Load(kc.cluster)
	ce := c.(cacheEntry)
	if ce.version == "" {
		kc.Get("/api", overrideAcceptWithJson)
		if kc.err == nil {
			ce.version, kc.err = yjq.JqEval(`.versions[-1] // ""`, kc.resp)
		}
		if kc.err != nil || ce.version == "" {
			logger.Error("unable to get version", zap.Error(kc.err))
		} else {
			cache.Store(kc.cluster, ce)
		}
	}
	return ce.version
}

// gvk == "gv:k" in map
// map is "gv:k" -> "name:namespaced"
func (kc *kc) GetApiResourceNameNamespacedFromGvk(gv string, k string) (string, string, error) {
	c, _ := cache.Load(kc.cluster)
	ce := c.(cacheEntry)
	nameNamespaced, present := ce.gvkMapNameNamespaced[gv+":"+k]
	if !present {
		resUrl := "/api/" + gv
		if gv != kc.Version() {
			resUrl = "/apis/" + gv
		}
		r, err := kc.Get(resUrl, overrideAcceptWithYaml)
		if err != nil {
			return "", "", err
		}
		rl, err := yjq.Eval2List(yjq.YqEval, `.resources[] | select(.name | contains("/") | not) | .kind + "," + .name + ":" + .namespaced`, r)
		if err != nil {
			return "", "", err
		}
		for _, le := range rl {
			kn := strings.Split(le, ",")
			_k := kn[0]
			_n := kn[1]
			ce.gvkMapNameNamespaced[gv+":"+_k] = _n
			if _k == k {
				nameNamespaced = _n
				present = true
			}
		}
	}
	if !present {
		return "", "", fmt.Errorf("name not found for groupVersion=%s and kind=%s", gv, k)
	}
	r := strings.Split(nameNamespaced, ":")
	return r[0], r[1], nil
}

func (kc *kc) SetGetParams(queryParams map[string]string) Kc {
	kc.client.SetQueryParams(queryParams)
	return kc
}

func (kc *kc) SetGetParam(name string, value string) Kc {
	kc.client.SetQueryParam(name, value)
	return kc
}

func logResponse(api string, resp *resty.Response) {
	if logger.Level() != zapcore.DebugLevel {
		return
	}
	zapFields := []zap.Field{
		zap.String("req", api),
		zap.Int("status code", resp.StatusCode()),
		zap.String("status", resp.Status()),
		zap.String("proto", resp.Proto()),
		zap.Int64("time", resp.Time().Milliseconds()),
		zap.Time("received at", resp.ReceivedAt()),
	}
	for name, values := range resp.Header() {
		for _, value := range values {
			zapFields = append(zapFields, zap.String(name, value))
		}
	}
	logger.Debug("http resp", zapFields...)
}

func (kc *kc) send(method string, apiCall string, yamlBody string) (string, error) {
	kc.api = apiCall
	kc.resp = ""
	kc.err = nil
	var (
		res *resty.Response
		req = kc.client.SetHeader("Accept", kc.accept).R()
	)
	switch {
	case method == http.MethodPatch:
		res, kc.err = req.
			SetBody(yamlBody).
			SetQueryParam("fieldManager", "skc-client-side-apply").
			SetQueryParam("fieldValidation", "Ignore").
			SetHeader("Content-Type", "application/apply-patch+yaml").
			Patch(apiCall)
	case method == http.MethodPost:
		res, kc.err = req.
			SetBody(yamlBody).
			SetQueryParam("fieldManager", "skc-client-side-apply").
			SetQueryParam("fieldValidation", "Ignore").
			SetHeader("Content-Type", "application/yaml").
			Post(apiCall)
	case method == http.MethodPut:
		yamlBody, kc.err = kc.setResourceVersion(apiCall, yamlBody)
		if kc.err == nil {
			res, kc.err = req.
				SetBody(yamlBody).
				SetHeader("Content-Type", "application/yaml").
				Put(apiCall)
		}
	}
	if kc.err == nil {
		logResponse(apiCall, res)
		kc.resp = string(res.Body())
		kc.status = res.Status()
		kc.statusCode = res.StatusCode()
	}
	return kc.resp, kc.err
}

func (kc *kc) setResourceVersion(apiCall string, newResource string) (string, error) {
	r, err := kc.Get(apiCall, overrideAcceptWithJson)
	if err != nil {
		return "", err
	}
	rv, err := yjq.JqEval(`.metadata.resourceVersion`, r)
	if err != nil {
		return "", err
	}
	nr, err := yjq.YqEval(`.metadata.resourceVersion = "`+rv+`"`, newResource)
	if err != nil {
		return "", err
	}
	return nr, nil
}

func getGvkNsNameFromYamlDoc(yaml string) (string, string, string, string, error) {
	r, e := yjq.YqEval(`.apiVersion + "," + .kind + "," + (.metadata.namespace // "") + "," + .metadata.name`, yaml)
	if e != nil {
		return "", "", "", "", e
	}
	_r := strings.Split(r, ",")
	if len(_r) != 4 {
		return "", "", "", "", fmt.Errorf("cannot extract groupVersion, kind, namespace, name from yaml document")
	}
	return _r[0], _r[1], _r[2], _r[3], nil
}
