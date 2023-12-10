package rest

import (
	"errors"
	"io"
	"net/http"

	"cloud_native/pkg/store"
	"github.com/gorilla/mux"
)

func (s *Server) AddRoutes() {
	// Basic Handler
	s.HandleFunc("/", s.helloGoHandler())

	// Key-Value store endpoints
	s.HandleFunc("/{key}", s.putKeyIntoStoreHandler()).Methods("PUT")
	s.HandleFunc("/{key}", s.getKeyValueHandler()).Methods("GET")
	s.HandleFunc("/{key}", s.deleteKeyValueHandler()).Methods("DELETE")
}

func (s *Server) helloGoHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello Key-Value store! \n"))
	}
}

func (s *Server) putKeyIntoStoreHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		key := vars["key"]

		value, err := io.ReadAll(r.Body)
		defer r.Body.Close()

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		err = store.Put(key, string(value))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		s.transactionLog.WritePut(key, string(value))

		w.WriteHeader(http.StatusCreated)
	}
}

func (s *Server) getKeyValueHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		key := vars["key"]

		value, err := store.Get(key)
		if errors.Is(err, store.ErrNoSuchKey) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write([]byte(value))
	}
}

func (s *Server) deleteKeyValueHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)

		key := vars["key"]

		err := store.Delete(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		s.transactionLog.WriteDelete(key)

		w.WriteHeader(http.StatusNoContent)
	}
}
