package bqloader_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"go.nownabe.dev/bqloader"
)

type roundTripperFunc func(req *http.Request) *http.Response

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

func newTestClient(f roundTripperFunc) *http.Client {
	return &http.Client{Transport: f}
}

type slackMessage struct {
	Channel   string `json:"channel"`
	IconEmoji string `json:"icon_emoji,omitempty"`
	Text      string `json:"text"`
	Username  string `json:"username,omitempty"`
}

const validSlackToken = "validToken"

func newSlackClient() *http.Client {
	return newTestClient(func(req *http.Request) *http.Response {
		resBody := func() string {
			if req.Header.Get("Authorization") != "Bearer "+validSlackToken {
				return `{"ok":false,"error":"not_authed"}`
			}

			reqBody, err := ioutil.ReadAll(req.Body)
			if err != nil {
				return ""
			}

			var msg slackMessage
			if err := json.Unmarshal(reqBody, &msg); err != nil {
				return `{"ok":false,"error":"invalid_form_data"}`
			}

			if msg.Channel == "" {
				return `{"ok":false,"error":"channel_not_found"}`
			}

			return `{"ok":true}`
		}()

		if resBody == "" {
			return &http.Response{StatusCode: http.StatusInternalServerError}
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewBufferString(resBody)),
			Header:     http.Header{},
		}
	})
}

func TestSlackNotifier(t *testing.T) {
	result := &bqloader.Result{
		Event:   bqloader.Event{Name: "testfile"},
		Handler: &bqloader.Handler{Name: "myhandler"},
	}

	cases := map[string]struct {
		notifier         *bqloader.SlackNotifier
		result           *bqloader.Result
		expectedHasError bool
	}{
		"succeeded": {
			notifier:         &bqloader.SlackNotifier{Channel: "#channel", Token: validSlackToken},
			result:           result,
			expectedHasError: false,
		},
		"failed": {
			notifier: &bqloader.SlackNotifier{Channel: "#channel", Token: validSlackToken},
			result: &bqloader.Result{
				Event:   bqloader.Event{Name: "testfile"},
				Handler: &bqloader.Handler{Name: "myhandler"},
				Error:   fmt.Errorf("some error"),
			},
			expectedHasError: false,
		},
		"no token": {
			notifier:         &bqloader.SlackNotifier{Channel: "#channel"},
			result:           result,
			expectedHasError: true,
		},
		"with options": {
			notifier: &bqloader.SlackNotifier{
				Channel:   "#channel",
				Token:     validSlackToken,
				IconEmoji: ":poop:",
				Username:  "username",
			},
			result:           result,
			expectedHasError: false,
		},
	}

	slackClient := newSlackClient()

	for name, c := range cases {
		c.notifier.HTTPClient = slackClient

		t.Run(name, func(t *testing.T) {
			err := c.notifier.Notify(context.Background(), c.result)
			if c.expectedHasError && err == nil {
				t.Errorf("expected error but no error occurred")
			}
			if !c.expectedHasError && err != nil {
				t.Errorf("unexpected error: %s", err)
			}
		})
	}
}
