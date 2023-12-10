package rest

import (
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

type Server struct {
	*mux.Router
	transactionLog TransactionLogger
}

func NewServer(transactionLog TransactionLogger) *Server {
	r := mux.NewRouter()

	api := r.PathPrefix("/v1").Subrouter()

	srv := &Server{
		Router:         api,
		transactionLog: transactionLog,
	}

	srv.AddRoutes()

	return srv
}
