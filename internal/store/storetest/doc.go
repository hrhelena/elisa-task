// Package storetest starts throwaway Postgres containers for integration
// tests (build tag integration). It is imported only by tests, so neither it
// nor testcontainers-go ends up in the service binary.
package storetest
