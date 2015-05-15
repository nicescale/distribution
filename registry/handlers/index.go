package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/docker/distribution/registry/index"
	"github.com/gorilla/handlers"
)

// indexDispatcher constructs ihe index handler api endpoint.
func indexDispatcher(ctx *Context, r *http.Request) http.Handler {
	indexHandler := &indexHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		"GET":   http.HandlerFunc(indexHandler.GetPage),
		"PATCH": http.HandlerFunc(indexHandler.SetTagStatus),
	}
}

// indexHandler handles requests for lists of index under a repository name.
type indexHandler struct {
	*Context
}

// GetTags returns a json list of index for a specific image name.
func (ih *indexHandler) GetPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	r.ParseForm()
	skip, _ := strconv.Atoi(r.Form.Get("skip"))
	limit, _ := strconv.Atoi(r.Form.Get("limit"))
	keyword := r.Form.Get("keyword")
	queryArgs := index.QueryArgs{
		Keyword: keyword,
		Skip:    skip,
		Limit:   limit,
	}

	page, err := ih.index.GetPage(queryArgs)
	if err == nil {
		enc := json.NewEncoder(w)
		err = enc.Encode(page)
	}

	if err != nil {
		ih.Errors.PushErr(err)
		return
	}
}

func (ih *indexHandler) SetTagStatus(w http.ResponseWriter, r *http.Request) {
	req := make(map[string]string)
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	err = ih.index.SetTagStatus(req["repo"], req["tag"], req["status"])
	if err == sql.ErrNoRows {
		http.Error(w, err.Error(), 404)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.WriteHeader(204)
}
