package kc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
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

// cmd exec in pod:
// -XPOST  -H "X-Stream-Protocol-Version: v4.channel.k8s.io" -H "X-Stream-Protocol-Version: v3.channel.k8s.io" -H "X-Stream-Protocol-Version: v2.channel.k8s.io" -H "X-Stream-Protocol-Version: channel.k8s.io" -H "User-Agent: oc/4.11.0 (linux/amd64) kubernetes/262ac9c" 'https://192.168.49.2:8443/api/v1/namespaces/default/pods/dumpdb-866cfc54f4-s9szl/exec?command=date&container=dumpdb&stderr=true&stdout=true'
//
// cp to pod:
// -XPOST  -H "X-Stream-Protocol-Version: v4.channel.k8s.io" -H "X-Stream-Protocol-Version: v3.channel.k8s.io" -H "X-Stream-Protocol-Version: v2.channel.k8s.io" -H "X-Stream-Protocol-Version: channel.k8s.io" -H "User-Agent: oc/4.11.0 (linux/amd64) kubernetes/262ac9c" 'https://192.168.49.2:8443/api/v1/namespaces/default/pods/dumpdb-866cfc54f4-s9szl/exec?command=tar&command=-xmf&command=-&command=-C&command=%2Ftmp&container=dumpdb&stderr=true&stdin=true&stdout=true'

const (
	Json                     = "json"
	Yaml                     = "yaml"
	TokenPath                = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	CurrentContext           = ".current-context"
	queryContextForCluster   = `(%s as $c | .contexts[] | select (.name == $c)).context as $ctx | $ctx | parent | parent | parent | .clusters[] | select(.name == $ctx.cluster) | .cluster.server // ""`
	queryContextForUserAuth  = `(%s as $c | .contexts[] | select (.name == $c)).context as $ctx | $ctx | parent | parent | parent | .users[] | select(.name == $ctx.user) | .user.%s`
	createModifierExtraParam = "?modifier_removed_name="
)

var (
	logger                 = log.Logger().Named("kcdump")
	cache                  sync.Map
	overrideAcceptWithJson map[string]string
	overrideAcceptWithYaml map[string]string
)

type (
	// Kc represents a kubernetes client
	Kc interface {
		SetToken(token string) Kc
		SetCert(cert tls.Certificate) Kc
		SetCluster(cluster string) Kc
		Get(apiCall string, headers ...map[string]string) (string, error)
		modify(modifier modifier, yamlManifest string, ignoreNotFound bool, isCreating bool, namespaces ...string) (string, error)

		// regular k8s apply for a given manifest over zero or more namespaces.
		// if first namespace item is "*" will apply to all namespaces.
		// if manifest document(s) has '.metadata.namespace' other namespaces are ignored
		ApplyManifest(yamlManifest string, namespaces ...string) (string, error)
		Apply(apiCall string, body string) (string, error)
		applyModifier(apiUrl string, body string, not_used bool) (string, error)
		CreateManifest(yamlManifest string, applyIfFound bool, namespaces ...string) (string, error)
		Create(apiCall string, body string, applyIfFound bool) (string, error)

		// regular k8s replace for a given manifest over zero or more namespaces.
		// if first namespace item is "*" will replace in all namespaces.
		// if manifest document(s) has '.metadata.namespace' other namespaces are ignored
		ReplaceManifest(yamlManifest string, applyIfNotFound bool, namespaces ...string) (string, error)
		Replace(apiCall string, body string, applyIfNotFound bool) (string, error)
		DeleteManifest(yamlManifest string, ignoreNotFound bool, namespaces ...string) (string, error)
		Delete(apiCall string, ignoreNotFound bool) (string, error)
		deleteModifier(apiUrl string, not_used string, ignoreNotFound bool) (string, error)

		// target format = namespace/pod/container. if container is omitted the first is used.
		// pod name can be a prefix in such case first found pod name with that suffix from the
		// replica list is used. returns stdout + stderr
		Exec(target string, cmd []string) (string, error)

		// src (source) and dst (destination) can be file:/absolute_path_to_local_file or
		// pod:/namespace/pod/container:/absolute_path_to_file_in_container
		// if 'pod:/' or 'file:/' as destination have trailing "/" the source file name is used
		// as file name and written into that path assumed to be a directory.
		// copy operations needs the targeted container to have the 'dd' utility in its path. if
		// not in path you can set its absolute path location with SetDDpath().
		// in 'pod:/[...]' the pod name can be a prefix for which 1st found pod is used if
		// more than one replica exists. container is optional and when this happens the 1st found
		// is used. if pod file destination is not specified while copying to pod '/tmp'
		// directory will be used as target.
		Copy(src string, dst string) error

		// 'dd' utility for copy operation if not in container path. default is "dd"
		// as in (...)/exec?container=dumpdb&command=dd(...)&stdout=true&stderr=true&stdin=true"
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
		Dump(path string, nsExclusionList []string, gvkExclusionList []string, syncChunkMap map[string]int, asyncChunkMap map[string]int, nologs bool, gz bool, tgz bool, prune bool, splitns bool, splitgv bool, format int, poolSize int, chunkSize int, escapeEncodedJson bool, copyToPod string, progress func()) error
		setCert(cert []byte, key []byte)
		send(method string, apiCall string, body string) (string, error)
		setResourceVersion(apiCall string, newResource string) (string, error)
		GetApiResourceNameNamespacedFromGvk(gv string, k string) (string, string, error)
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
	kc := newKc()
	if context != CurrentContext {
		context = `"` + context + `"`
	}
	logger.Debug("context " + context)
	kcfg, err := os.ReadFile(config)
	if err != nil {
		logger.Error("reading config file. will try reading from stdin...", zap.Error(err))
		stdin, stdinerr := io.ReadAll(os.Stdin)
		if stdinerr != nil {
			logger.Error("reading config from stdin also failed.", zap.Error(err))
			return nil
		} else {
			kcfg = stdin
		}
	}
	kubeCfg := string(kcfg)
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

func getCopyParams(kc *kc, src string, dst string) (bool, string, string, string, string, string, error) {
	src = strings.Trim(src, " ")
	dst = strings.Trim(dst, " ")
	logger.Debug("src=" + src + " dst=" + dst)
	if (!strings.HasPrefix(src, "file:/") && !strings.HasPrefix(src, "pod:/")) ||
		(!strings.HasPrefix(dst, "file:/") && !strings.HasPrefix(dst, "pod:/")) {
		return false, "", "", "", "", "", errors.New("source and or destiny does not follow 'file:/' 'pod:/' pattern")
	}
	if (strings.HasPrefix(src, "file:/") && strings.HasPrefix(dst, "file:/")) ||
		(strings.HasPrefix(src, "pod:/") && strings.HasPrefix(dst, "pod:/")) {
		return false, "", "", "", "", "", errors.New("source and destiny cannot be both 'file:/' or both 'pod:/'")
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
		return false, "", "", "", "", "", errors.New("pattern 'file:/' in copyTo used with non file (directory?) or inexistent")
	}
	namespace, pod, container, err := getPodTarget(remote)
	if err != nil {
		return false, "", "", "", "", "", err
	}
	logger.Debug("namespace=" + namespace + " pod=" + pod + " container=" + container)
	container_split := strings.Split(container, ":")
	logger.Debug("", zap.Strings("container_split", container_split))
	container = container_split[0]
	if len(container_split) == 1 {
		if !sending {
			return false, "", "", "", "", "", errors.New("absent pod source file in pod:/" + remote)
		}
		logger.Info("no directory specified, copying to /tmp")
		podFile = "/tmp/" + filepath.Base(localFile)
	} else {
		podFile = container_split[1]
		if strings.HasSuffix(podFile, "/") {
			if !sending {
				return false, "", "", "", "", "", errors.New("absent pod source file in pod:/" + remote)
			}
			podFile = podFile + filepath.Base(localFile)
		}
	}
	logger.Sugar().Debug("sending=", sending, " localfile=", localFile)
	if !sending && strings.HasSuffix(localFile, "/") {
		localFile = localFile + filepath.Base(podFile)
	}
	_pod, _container := getPodAndContainer(kc, namespace, pod, container)
	logger.Debug("getPodAndContainer ----> namespace=" + namespace + " _pod=" + pod + " container=" + _container)
	if _pod == "" || _container == "" {
		return false, "", "", "", "", "", fmt.Errorf("not found pod=%s container=%s", pod, container)
	}
	return sending, localFile, podFile, namespace, _pod, _container, nil
}

func (kc *kc) Copy(src string, dst string) error {
	sending, localFile, podFile, namespace, pod, container, err := getCopyParams(kc, src, dst)
	if err != nil {
		kc.err = err
		return err
	}
	stdin := false
	fileSizeReportedToDD := int64(-1)
	queryCmd := fmt.Sprintf("&command=%s&command=%s&command=%s", kc.ddPath, url.QueryEscape("bs=1"), url.QueryEscape("if="+podFile))
	if sending {
		stdin = true
		info, err := os.Stat(localFile)
		if err != nil {
			return err
		}
		fileSizeReportedToDD = info.Size() //int64(4096) //info.Size()
		queryCmd = fmt.Sprintf("&command=%s&command=%s&command=%s&command=%s%d", kc.ddPath, url.QueryEscape("of="+podFile), url.QueryEscape("bs=1"), url.QueryEscape("count="), fileSizeReportedToDD)
	}
	logger.Debug("copy cmd", zap.Bool("isSending", sending), zap.String("reqCmd", queryCmd))
	wsConn, resp, err := getWsConn(kc, namespace, pod, container, queryCmd, stdin)
	if err != nil {
		logger.Error("copy dial ws conn:", zap.Error(err))
		return err
	}
	defer wsConn.Close()
	for k, v := range resp.Header {
		logger.Debug("copy resp headers", zap.Strings(k, v))
	}
	if sending {
		return copyTo(wsConn, localFile, fileSizeReportedToDD)
	}
	return copyFrom(wsConn, localFile, src)
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
		return fmt.Errorf("file stat copyFrom. localfile=%s" + localFile)
	}
	if !strings.Contains(stdErr.String(), fmt.Sprintf("%d bytes", info.Size())) {
		return fmt.Errorf("%s file size = %d. different from dd stderr = %s", localFile, info.Size(), stdErr.String())
	}
	if strings.Contains(stdErr.String(), "dd: error") {
		return fmt.Errorf("error copying from %s\n%s", src, stdErr.String())
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
		r, err := io.ReadFull(reader, b[1:])
		read += int64(r)
		if err == io.EOF {
			break
		}
		err = wsConn.WriteMessage(websocket.BinaryMessage, b[:r+1])
		if err != nil {
			logger.Debug("copyTo wsConn.WriteMessage " + err.Error())
			return err
		}
		if err == io.ErrUnexpectedEOF {
			break
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
		return fmt.Errorf("file stat copyFrom. localfile=%s" + localFile)
	}
	if !strings.Contains(stdErr.String(), fmt.Sprintf("%d bytes", info.Size())) {
		return fmt.Errorf("%s file size = %d. different from dd stderr\nif trying to copy to a directory add '/' to the end of pod destination\n%s", localFile, info.Size(), stdErr.String())
	}
	return nil
}

func (kc *kc) Exec(target string, cmd []string) (string, error) {
	namespace, pod, container, err := getPodTarget(target)
	if err != nil {
		return "", err
	}
	_pod, _container := getPodAndContainer(kc, namespace, pod, container)
	if _pod == "" || _container == "" {
		return "", fmt.Errorf("not found pod %s container %s", pod, container)
	}
	queryCmd := ""
	for _, c := range cmd {
		queryCmd = queryCmd + "&command=" + url.QueryEscape(c)
	}
	wsConn, resp, err := getWsConn(kc, namespace, _pod, _container, queryCmd, false)
	if err != nil {
		logger.Error("exec dial ws conn:", zap.Error(err))
		return "", err
	}
	defer wsConn.Close()
	for k, v := range resp.Header {
		logger.Debug("exec resp headers", zap.Strings(k, v))
	}
	var stdOutErr strings.Builder
	for {
		_, m, e := wsConn.ReadMessage()
		if e != nil {
			ce, ok := e.(*websocket.CloseError)
			if !ok || ce.Code != websocket.CloseNormalClosure {
				logger.Error("ws read:" + e.Error())
			}
			break
		}
		stdOutErr.WriteString(string(m[1:]))
	}
	return stdOutErr.String(), nil
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

func getPodTarget(target string) (string, string, string, error) {
	_target, path := target, ""
	if i := strings.Index(target, ":"); i != -1 {
		_target = target[0:i]
		path = target[i:]
	}
	logger.Debug("getPodTarget", zap.String("_target", _target), zap.String("path", path))
	targetList := strings.Split(_target, "/")
	if len(targetList) < 2 {
		return "", "", "", fmt.Errorf("wrong targe format %s. should be 'namespace/pod[/container]'", target)
	}
	namespace := targetList[0]
	pod := targetList[1]
	container := ""
	if len(targetList) > 2 {
		container = targetList[2]
	}
	return namespace, pod, container + path, nil
}

func getPodAndContainer(kc *kc, ns string, podPrefix string, container string) (string, string) {
	pods, e := kc.Get("/api/"+kc.Version()+"/namespaces/"+ns+"/pods", overrideAcceptWithYaml)
	if e != nil {
		logger.Error("", zap.Error(e))
		return "", ""
	}
	list, e := yjq.Eval2List(yjq.YqEval, `.items[] | .metadata.name + "," + .spec.containers[0].name`, pods)
	if e != nil {
		logger.Error("", zap.Error(e))
		return "", ""
	}
	pod, _container := "", ""
	for _, p := range list {
		item := strings.Split(p, ",")
		if strings.HasPrefix(item[0], podPrefix) {
			pod = item[0]
			_container = item[1]
			break
		}
	}
	if container != "" {
		_container = container
	}
	return pod, _container
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

func (kc *kc) Apply(apiCall string, body string) (string, error) {
	return kc.applyModifier(apiCall, body, false)
}

func (kc *kc) ApplyManifest(yamlManifest string, namespaces ...string) (string, error) {
	return kc.modify(kc.applyModifier, yamlManifest, false, false, namespaces...)
}

func (kc *kc) applyModifier(apiUrl string, body string, not_used bool) (string, error) {
	logger.Debug("apply", zap.String("apiCall", apiUrl))
	return kc.send(http.MethodPatch, apiUrl, body)
}

func (kc *kc) Create(apiCall string, body string, applyIfFound bool) (string, error) {
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

func (kc *kc) CreateManifest(yamlManifest string, applyIfFound bool, namespaces ...string) (string, error) {
	return kc.modify(kc.Create, yamlManifest, applyIfFound, true, namespaces...)
}

func (kc *kc) ReplaceManifest(yamlManifest string, applyIfNotFound bool, namespaces ...string) (string, error) {
	return kc.modify(kc.Replace, yamlManifest, applyIfNotFound, false, namespaces...)
}

func (kc *kc) Replace(apiCall string, body string, applyIfNotFound bool) (string, error) {
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

func (kc *kc) Delete(apiCall string, ignoreNotFound bool) (string, error) {
	return kc.deleteModifier(apiCall, "", ignoreNotFound)
}

func (kc *kc) DeleteManifest(yamlManifest string, ignoreNotFound bool, namespaces ...string) (string, error) {
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
			return []string{}, errors.New("no namespace specified") // use default? no
		}
		if namespaces[0] == "*" {
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
