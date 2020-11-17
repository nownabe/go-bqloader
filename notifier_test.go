package bqloader_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	"go.nownabe.dev/bqloader"
)

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func newTestClient(f roundTripperFunc) *http.Client {
	return &http.Client{Transport: f}
}

func TestSlackNotifier(t *testing.T) {
	client := newTestClient(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewBufferString(`{"ok":true}`)),
			Header:     http.Header{},
		}, nil
	})

	n := &bqloader.SlackNotifier{
		Channel:    "#channel",
		Token:      "token",
		IconEmoji:  ":emoji:",
		Username:   "username",
		HTTPClient: client,
	}

	r := &bqloader.Result{
		Event:   bqloader.Event{Name: "testfile"},
		Handler: &bqloader.Handler{Name: "myhandler"},
	}

	err := n.Notify(context.Background(), r)
	if err != nil {
		t.Errorf("unexpected slack.Notify error: %s", err)
	}
}
