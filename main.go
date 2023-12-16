package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"cloud_native/pkg/store"
	"cloud_native/pkg/transcationlog"
	"github.com/gorilla/mux"
)

type TransactionLogger interface {
	WritePut(key, value string)
	WriteDelete(key string)
	Err() <-chan error
	ReadEvents() (<-chan transcationlog.Event, <-chan error)
	Run()
	Close() error
}

var transact TransactionLogger

func main() {
	fmt.Println("Starting the server")
	r := mux.NewRouter()

	err := initializeTransactionLog()
	if err != nil {
		panic(err)
	}

	api := r.PathPrefix("/v1").Subrouter()

	api.HandleFunc("/", helloGoHandler)
	api.HandleFunc("/{key}", putKeyIntoStoreHandler).Methods("PUT")
	api.HandleFunc("/{key}", getKeyValueHandler).Methods("GET")
	api.HandleFunc("/{key}", deleteKeyValueHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServeTLS(":8080", "cert.pem", "key.pem", r))
}

func helloGoHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hello Key-Value store! \n"))
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

	transact.WritePut(key, string(value))

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

	transact.WriteDelete(key)

	w.WriteHeader(http.StatusNoContent)
}

func initializeTransactionLog() error {
	var err error

	transact, err = transcationlog.NewFileTransactionLog("transaction.log")
	//transact, err = transcationlog.NewPostgresTransactionLog(transcationlog.PostgresDBParams{
	//	DbName:   "postgres",
	//	Host:     "localhost",
	//	User:     "admin",
	//	Password: "admin",
	//})
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errs := transact.ReadEvents()
	e, ok := transcationlog.Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errs:
		case e, ok = <-events:
			switch e.EventType {
			case transcationlog.EventDelete:
				err = store.Delete(e.Key)
			case transcationlog.EventPut:
				err = store.Put(e.Key, e.Value)
			}
		}
	}

	transact.Run()

	return err
}
