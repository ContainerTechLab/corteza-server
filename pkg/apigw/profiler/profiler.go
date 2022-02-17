package profiler

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"time"

	actx "github.com/cortezaproject/corteza-server/pkg/apigw/ctx"
	h "github.com/cortezaproject/corteza-server/pkg/http"
	"github.com/davecgh/go-spew/spew"
)

type (
	Hits map[string][]*Hit

	Profiler struct {
		l Hits
	}

	Hit struct {
		ID string
		R  *h.Request
		Ts *time.Time
		Tf *time.Time
		D  *time.Duration
	}

	CtxHit []*Stage

	Stage struct {
		Name string
		Ts   *time.Time
		Tf   *time.Time
	}

	Sort struct {
		Path   string
		Size   uint64
		Before *time.Time
	}
)

func New() *Profiler {
	return &Profiler{make(Hits)}
}

func (p *Profiler) Hit(r *h.Request) (h *Hit) {
	var (
		n = time.Now()
	)

	h = &Hit{"", r, &n, nil, nil}
	h.generateID()

	return
}

func (p *Profiler) Push(h *Hit) (id string) {
	if h.Tf == nil {
		n := time.Now()
		d := n.Sub(*h.Ts)

		h.Tf = &n
		h.D = &d
	}

	h.generateID()

	id = p.id(h.R)
	p.l[id] = append(p.l[id], h)

	return
}

func (p *Profiler) Dump(s Sort) Hits {
	return p.l.Filter(func(k string, v *Hit) bool {
		var b bool = true

		if s.Path != "" && v.R.URL.Path != s.Path {
			b = false
		}

		if s.Before != nil && !s.Before.IsZero() && v.Ts.Before(*s.Before) {
			b = false
		}

		return b
	})
}

func (p *Profiler) id(r *h.Request) string {
	return r.URL.Path
}

func (h *Hit) generateID() {
	h.ID = base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%s_%s", h.R.URL.Path, h.Ts)))
}

func (s Hits) Filter(fn func(k string, v *Hit) bool) Hits {
	ss := Hits{}

	for k, v := range s {
		for _, vv := range v {
			if !fn(k, vv) {
				continue
			}

			ss[k] = append(ss[k], vv)
		}
	}

	return ss
}

func StartHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		spew.Dump("Profiler start", time.Now())
		// add some info to context
		next.ServeHTTP(rw, r)
	})
}

func FinishHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		spew.Dump("Profiler finish", time.Now())
		spew.Dump("context of profiler", actx.ProfilerFromContext(r.Context()))
		next.ServeHTTP(rw, r)
	})
}
