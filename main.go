package main

import (
	"errors"
	"io"
	"log"
	"net/http"

	"cloud_native/store"
	"github.com/gorilla/mux"
)

func main() {
	r := mux.NewRouter()

	api := r.PathPrefix("/v1").Subrouter()

	api.HandleFunc("/", helloGoHandler)
	api.HandleFunc("/{key}", putKeyIntoStoreHandler).Methods("PUT")
	api.HandleFunc("/{key}", getKeyValueHandler).Methods("GET")
	api.HandleFunc("/{key}", deleteKeyValueHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}

func helloGoHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello Gorilla! \n"))
}

func putKeyIntoStoreHandler(w http.ResponseWriter, r *http.Request) {
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

	w.WriteHeader(http.StatusCreated)
}

func getKeyValueHandler(w http.ResponseWriter, r *http.Request) {
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

func deleteKeyValueHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	key := vars["key"]

	err := store.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type HelloStruct struct {
	ID string `json:"ID"`
	H  struct{ L string }
	L  []int
}
