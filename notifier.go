package bqloader

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
)

// Notifier notifies results for each event.
type Notifier interface {
	Notify(context.Context, *Result) error
}

// Result is a result for each event.
type Result struct {
	Event   Event
	Handler *Handler
	Error   error
}

// SlackNotifier is a notifier for Slack.
// SlackNotifier requires bot token and permissions.
// Recommended permissions are chat:write, chat:write.customize and chat:write.public.
type SlackNotifier struct {
	Channel string
	Token   string

	// Optional.
	IconEmoji string

	// Optional.
	Username string

	// Optional.
	HTTPClient *http.Client

	once sync.Once
}

type slackMessage struct {
	Channel   string `json:"channel"`
	IconEmoji string `json:"icon_emoji,omitempty"`
	Text      string `json:"text"`
	Username  string `json:"username,omitempty"`
}

type slackResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
}

// Notify notifies results to Slack channel.
func (n *SlackNotifier) Notify(ctx context.Context, r *Result) error {
	l := log.Ctx(ctx)

	n.once.Do(func() {
		if n.HTTPClient == nil {
			n.HTTPClient = &http.Client{}
		}
	})

	var text string
	if r.Error == nil {
		text = fmt.Sprintf(`:white_check_mark: %s handler successfully loaded %s`, r.Handler.Name, r.Event.Name)
	} else {
		text = fmt.Sprintf(`:x: %s handler failed to load %s: %s`, r.Handler.Name, r.Event.Name, r.Error)
	}
	m := &slackMessage{
		Channel:   n.Channel,
		IconEmoji: n.IconEmoji,
		Text:      text,
		Username:  n.Username,
	}
	l.Debug().Msgf("m = %+v", m)

	if err := n.postMessage(ctx, m); err != nil {
		return xerrors.Errorf("slack postMessage failed: %w", err)
	}

	return nil
}

func (n *SlackNotifier) postMessage(ctx context.Context, m *slackMessage) error {
	l := log.Ctx(ctx)

	reqJSON, err := json.Marshal(m)
	if err != nil {
		return xerrors.Errorf("failed to marshal json: %w", err)
	}

	req, err := http.NewRequest("POST", "https://slack.com/api/chat.postMessage", bytes.NewReader(reqJSON))
	if err != nil {
		return xerrors.Errorf("failed to build http request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	l.Debug().Msgf("req = %+v", req)
	req.Header.Set("Authorization", "Bearer "+n.Token)

	resp, err := n.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return xerrors.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	l.Debug().Msgf("resp = %+v", resp)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return xerrors.Errorf("failed to read response body: %w", err)
	}

	l.Debug().Msgf("body = %s", body)

	if resp.StatusCode >= http.StatusBadRequest {
		return xerrors.Errorf(
			"slack webhook request failed with status code %d (%s)", resp.StatusCode, body)
	}

	var sres slackResponse
	if err := json.Unmarshal(body, &sres); err != nil {
		return xerrors.Errorf("failed to unmarshal response body: %w", err)
	}

	if !sres.OK {
		return xerrors.Errorf("failed to send message: %s", sres.Error)
	}

	return nil
}
