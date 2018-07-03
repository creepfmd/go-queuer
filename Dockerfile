FROM golang:latest

RUN mkdir /app
ADD . /app/
WORKDIR /app
RUN go get -d -v ./...
RUN go build -o main .

EXPOSE 8080

CMD ["/app/main"]
