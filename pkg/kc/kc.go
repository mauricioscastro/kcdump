package kc

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mauricioscastro/kcdump/pkg/util/log"
	"github.com/mauricioscastro/kcdump/pkg/yjq"
)

const (
	TokenPath               = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	CurrentContext          = ".current-context"
	queryContextForCluster  = `(%s as $c | .contexts[] | select (.name == $c)).context as $ctx | $ctx | parent | parent | parent | .clusters[] | select(.name == $ctx.cluster) | .cluster.server // ""`
	queryContextForUserAuth = `(%s as $c | .contexts[] | select (.name == $c)).context as $ctx | $ctx | parent | parent | parent | .users[] | select(.name == $ctx.user) | .user.%s`
)

var (
	logger = log.Logger().Named("hcr.kc")
	cache  sync.Map
)

type (
	// Kc represents a kubernetes client
	Kc interface {
		SetToken(token string) Kc
		SetCert(cert tls.Certificate) Kc
		SetCluster(cluster string) Kc
		Get(apiCall string) (string, error)
		GetJson(apiCall string) (string, error)
		Apply(apiCall string, body string) (string, error)
		Create(apiCall string, body string) (string, error)
		Replace(apiCall string, body string) (string, error)
		Delete(apiCall string, ignoreNotFound bool) (string, error)
		SetGetParams(queryParams map[string]string) Kc
		SetGetParam(name string, value string) Kc
		SetResponseTransformer(transformer ResponseTransformer) Kc
		Version() string
		Response() string
		Err() error
		Status() int
		Cluster() string
		Api() string
		Ns() (string, error)
		ApiResources() (string, error)
		Dump(path string, nsExclusionList []string, gvkExclusionList []string, nologs bool, gz bool, tgz bool, prune bool, splitns bool, splitgv bool, format int, poolSize int, progress func()) error
		setCert(cert []byte, key []byte)
		response(resp *resty.Response, yamlOutput bool) (string, error)
		send(method string, apiCall string, body string) (string, error)
		get(apiCall string, yamlOutput bool) (string, error)
		setResourceVersion(apiCall string, newResource string) (string, error)
	}

	kc struct {
		client      *resty.Client
		readOnly    bool
		cluster     string
		api         string
		resp        string
		err         error
		status      int
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

func NewKc() Kc {
	token, err := os.ReadFile(TokenPath)
	if err != nil {
		logger.Debug("could not read default sa token. skipping to kubeconfig", zap.Error(err))
	} else {
		logger.Debug("", zap.String("token from sa account", "XXX"))
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
		if host != "" && port != "" {
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
			logger.Error("empty user cert info reading context. nothing else to try for auth. returning...")
			return nil
		}
		logger.Debug("", zap.String("cert", cert))
		key, err := yjq.YqEval(fmt.Sprintf(queryContextForUserAuth, context, `client-key-data // "" | @base64d`), kubeCfg)
		if err != nil {
			logger.Error("reading user cert key info for context", zap.Error(err))
			return nil
		}
		if len(key) == 0 {
			logger.Error("empty user cert key info reading context. nothing else to try for auth. returning...")
			return nil
		}
		logger.Debug("", zap.String("key", "XXX"))
		kc.setCert([]byte(cert), []byte(key))
	} else {
		logger.Debug("", zap.String("token from kube config", "XXX"))
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
	yjq.SilenceYqLogs()
	return &kc
}

func (kc *kc) SetCluster(cluster string) Kc {
	kc.cluster = cluster
	kc.client.SetBaseURL(cluster)
	cache.Store(cluster, cacheEntry{""})
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

func (kc *kc) Status() int {
	return kc.status
}

func (kc *kc) Api() string {
	return kc.api
}

func (kc *kc) GetJson(apiCall string) (string, error) {
	return kc.get(apiCall, false)
}

func (kc *kc) Get(apiCall string) (string, error) {
	return kc.get(apiCall, true)
}

func (kc *kc) get(apiCall string, yamlOutput bool) (string, error) {
	kc.api = apiCall
	resp, err := kc.client.R().Get(apiCall)
	if err != nil {
		return "", err
	}
	logResponse(apiCall, resp)
	kc.resp, kc.err = kc.response(resp, yamlOutput)
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
	return kc.response(resp, true)
}

func (kc *kc) Version() string {
	c, _ := cache.Load(kc.cluster)
	ce := c.(cacheEntry)
	if ce.version == "" {
		kc.GetJson("/api")
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

func (kc *kc) response(resp *resty.Response, yamlOutput bool) (string, error) {
	kc.status = resp.StatusCode()
	contentType := strings.ToLower(resp.Header().Get("Content-Type"))
	body := string(resp.Body())
	if resp.StatusCode() >= 400 {
		if strings.Contains(contentType, "json") && yamlOutput {
			if ymlBody, err := yjq.J2Y(body); err == nil {
				body = ymlBody
			}
		}
		return "", errors.New(resp.Status() + "\n" + body)
	}
	if strings.Contains(contentType, "json") {
		var err error
		if yamlOutput {
			body, err = yjq.J2Y(body)
		}
		if err != nil {
			return "", err
		}
	}
	return body, nil
}

func (kc *kc) send(method string, apiCall string, body string) (string, error) {
	if kc.readOnly {
		kc.resp, kc.err = "", errors.New("trying to write in read only mode")
		return kc.resp, kc.err
	}
	kc.api = apiCall
	var res *resty.Response
	switch {
	case http.MethodPatch == method:
		res, kc.err = kc.client.R().
			SetBody(body).
			SetQueryParam("fieldManager", "skc-client-side-apply").
			SetHeader("Content-Type", "application/apply-patch+yaml").
			Patch(apiCall)
	case http.MethodPost == method:
		res, kc.err = kc.client.R().
			SetBody(body).
			SetHeader("Content-Type", "application/yaml").
			Post(apiCall)
	case http.MethodPut == method:
		body, kc.err = kc.setResourceVersion(apiCall, body)
		if kc.err == nil {
			res, kc.err = kc.client.R().
				SetBody(body).
				SetHeader("Content-Type", "application/yaml").
				Put(apiCall)
		}
	}
	if kc.err == nil {
		logResponse(apiCall, res)
		kc.resp, kc.err = kc.response(res, true)
	}
	return kc.resp, kc.err
}

func (kc *kc) setResourceVersion(apiCall string, newResource string) (string, error) {
	r, err := kc.GetJson(apiCall)
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
