PKG=$(shell glide nv)

default: vet test

test:
	go test $(PKG)

vet:
	go vet $(PKG)

bench:
	go test $(PKG) -run=NONE -bench=. -benchmem -benchtime=5s

doc: README.md

README.md: README.md.tpl $(wildcard *.go)
 	# go get -u github.com/davelondon/rebecca/cmd/becca
	becca -package .
