package kc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/coreybutler/go-fsutil"
	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mauricioscastro/kcdump/pkg/util/log"
	"github.com/mauricioscastro/kcdump/pkg/yjq"
)

// cmd exec in pod:
// 'https://cluster:443/api/v1/namespaces/exec/pods/shell-demo/exec?command=ls&container=loop&stderr=true&stdout=true'
//
// cp to pod:
// -H "X-Stream-Protocol-Version: v4.channel.k8s.io" -H "X-Stream-Protocol-Version: v3.channel.k8s.io" -H "X-Stream-Protocol-Version: v2.channel.k8s.io" -H "X-Stream-Protocol-Version: channel.k8s.io" 'https://192.168.49.2:8443/api/v1/namespaces/hcr/pods/dumpdb-59f55f8cc7-lqp7n/exec?command=tar&command=-xmf&command=-&command=-C&command=%2Ftmp&container=dumpdb&stderr=true&stdin=true&stdout=true'

const (
	Json                    = "json"
	Yaml                    = "yaml"
	TokenPath               = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	CurrentContext          = ".current-context"
	queryContextForCluster  = `(%s as $c | .contexts[] | select (.name == $c)).context as $ctx | $ctx | parent | parent | parent | .clusters[] | select(.name == $ctx.cluster) | .cluster.server // ""`
	queryContextForUserAuth = `(%s as $c | .contexts[] | select (.name == $c)).context as $ctx | $ctx | parent | parent | parent | .users[] | select(.name == $ctx.user) | .user.%s`
)

var (
	logger = log.Logger().Named("kcdump")
	cache  sync.Map
)

type (
	// Kc represents a kubernetes client
	Kc interface {
		SetToken(token string) Kc
		SetCert(cert tls.Certificate) Kc
		SetCluster(cluster string) Kc
		Get(apiCall string) (string, error)
		Apply(apiCall string, body string) (string, error)
		Create(apiCall string, body string) (string, error)
		Replace(apiCall string, body string) (string, error)
		Delete(apiCall string, ignoreNotFound bool) (string, error)
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
		ApiResources() (string, error)
		Dump(path string, nsExclusionList []string, gvkExclusionList []string, syncChunkMap map[string]int, asyncChunkMap map[string]int, nologs bool, gz bool, tgz bool, prune bool, splitns bool, splitgv bool, format int, poolSize int, chunkSize int, escapeEncodedJson bool, progress func()) error
		setCert(cert []byte, key []byte)
		// response(resp *resty.Response, yamlOutput bool) (string, error)
		send(method string, apiCall string, body string) (string, error)
		// get(apiCall string, yamlOutput bool) (string, error)
		setResourceVersion(apiCall string, newResource string) (string, error)
	}

	kc struct {
		client      *resty.Client
		readOnly    bool
		cluster     string
		api         string
		resp        string
		err         error
		statusCode  int
		status      string
		accept      string
		transformer ResponseTransformer
	}
	// Optional transformer function to Get methods
	// should return ('transformed response', 'transformed error')
	ResponseTransformer func(Kc) (string, error)
	cacheEntry          struct {
		version string
	}
)

func init() {
	cache = sync.Map{} //currently only caching api version
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
	yjq.SilenceYqLogs()
	return &kc
}

func (kc *kc) SetCluster(cluster string) Kc {
	kc.cluster = cluster
	kc.client.SetBaseURL(kc.cluster)
	cache.Store(kc.cluster, cacheEntry{""})
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
	kc.client.SetCertificates(cert)
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

func (kc *kc) Get(apiCall string) (string, error) {
	kc.api = apiCall
	resp, err := kc.client.R().SetHeader("Accept", kc.accept).Get(apiCall)
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
	return kc.send(http.MethodPatch, apiCall, body)
}

func (kc *kc) Create(apiCall string, body string) (string, error) {
	return kc.send(http.MethodPost, apiCall, body)
}

func (kc *kc) Replace(apiCall string, body string) (string, error) {
	return kc.send(http.MethodPut, apiCall, body)
}

func (kc *kc) Delete(apiCall string, ignoreNotFound bool) (string, error) {
	if kc.readOnly {
		kc.resp, kc.err = "", errors.New("trying to delete in read only mode")
		return kc.resp, kc.err
	}
	kc.api = apiCall
	resp, err := kc.client.R().Delete(apiCall)
	if err != nil {
		kc.resp, kc.err = "", err
		return kc.resp, kc.err
	}
	logResponse(apiCall, resp)
	if resp.StatusCode() == http.StatusNotFound && ignoreNotFound {
		kc.resp, kc.err = "", nil
		return kc.resp, kc.err
	}
	kc.resp = string(resp.Body())
	if resp.StatusCode() >= 400 {
		kc.err = errors.New(resp.Status() + "\n" + kc.resp)
		return "", kc.err
	}
	kc.status = resp.Status()
	kc.statusCode = resp.StatusCode()
	return kc.resp, nil
}

func (kc *kc) Version() string {
	c, _ := cache.Load(kc.cluster)
	ce := c.(cacheEntry)
	if ce.version == "" {
		kc.Accept(Json).Get("/api")
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
	if kc.readOnly {
		kc.resp, kc.err = "", errors.New("trying to write in read only mode")
		return kc.resp, kc.err
	}
	kc.api = apiCall
	var res *resty.Response
	switch {
	case http.MethodPatch == method:
		res, kc.err = kc.client.R().
			SetBody(yamlBody).
			SetQueryParam("fieldManager", "skc-client-side-apply").
			SetHeader("Content-Type", "application/apply-patch+yaml").
			Patch(apiCall)
	case http.MethodPost == method:
		res, kc.err = kc.client.R().
			SetBody(yamlBody).
			SetHeader("Content-Type", "application/yaml").
			Post(apiCall)
	case http.MethodPut == method:
		yamlBody, kc.err = kc.setResourceVersion(apiCall, yamlBody)
		if kc.err == nil {
			res, kc.err = kc.client.R().
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
	r, err := kc.Accept(Json).Get(apiCall)
	if err != nil {
		return "", err
	}
	rv, err := yjq.JqEval(`.metadata.resourceVersion`, r)
	if err != nil {
		return "", err
	}
	nr, err := yjq.YqEvalJ2Y(`.metadata.resourceVersion = "`+rv+`"`, newResource)
	if err != nil {
		return "", err
	}
	return nr, nil
}
