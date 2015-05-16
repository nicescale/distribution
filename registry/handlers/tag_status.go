package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/gorilla/handlers"
)

// indexDispatcher constructs ihe index handler api endpoint.
func tagStatusDispatcher(ctx *Context, r *http.Request) http.Handler {
	tagStatusHandler := &tagStatusHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		"PATCH": http.HandlerFunc(tagStatusHandler.SetTagStatus),
	}
}

// indexHandler handles requests for lists of index under a repository name.
type tagStatusHandler struct {
	*Context
}

func (th *tagStatusHandler) SetTagStatus(w http.ResponseWriter, r *http.Request) {
	req := make(map[string]string)
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	err = th.index.SetTagStatus(req["repository"], req["tag"], req["status"], req["description"], req["target_url"])
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
