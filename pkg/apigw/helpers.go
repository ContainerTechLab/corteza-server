package apigw

import (
	"net/http"

	"github.com/cortezaproject/corteza-server/pkg/apigw/profiler"
	h "github.com/cortezaproject/corteza-server/pkg/http"
	"github.com/cortezaproject/corteza-server/pkg/options"
)

const (
	devHelperResponseBody string = `Hey developer!`
)

func helperDefaultResponse(opt *options.ApigwOpt) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if opt.LogEnabled {
			// Say something friendly when logging is enabled
			http.Error(w, devHelperResponseBody, http.StatusTeapot)
		} else {
			// Default 404 response
			http.Error(w, "", http.StatusNotFound)
		}
	}
}

func helperMethodNotAllowed(opt *options.ApigwOpt) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if opt.LogEnabled {
			// Say something friendly when logging is enabled
			http.Error(w, devHelperResponseBody, http.StatusTeapot)
		} else {
			// Default 405 response
			http.Error(w, "", http.StatusMethodNotAllowed)
		}
	}
}

func helperProfiler(opt *options.ApigwOpt, p *profiler.Profiler) func(http.HandlerFunc) http.HandlerFunc {
	return func(hf http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !opt.ProfilerEnabled {
				hf.ServeHTTP(w, r)
				return
			}

			// add to profiler
			ar, err := h.NewRequest(r)

			if err != nil {
				panic(err)
			}

			h := p.Hit(ar)

			hf.ServeHTTP(w, r)

			p.Push(h)
		}
	}
}
