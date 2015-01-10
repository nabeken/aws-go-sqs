// +build debug

package queue

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/motemen/go-loghttp"

	_ "github.com/motemen/go-loghttp/global"
)

func init() {
	loghttp.DefaultLogRequest = func(req *http.Request) {
		body, _ := ioutil.ReadAll(req.Body)
		defer req.Body.Close()

		req.Body = ioutil.NopCloser(bytes.NewReader(body))
		log.Printf("---> %s %s\n%s\n", req.Method, req.URL, body)
	}

	loghttp.DefaultLogResponse = func(resp *http.Response) {
		body, _ := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()

		resp.Body = ioutil.NopCloser(bytes.NewReader(body))
		log.Printf("<--- %d %s\n%s\n", resp.StatusCode, resp.Request.URL, body)
	}
}
