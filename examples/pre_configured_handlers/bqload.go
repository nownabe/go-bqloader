package bqload

import (
	"context"
	"os"
	"runtime"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

var loader bqloader.BQLoader

func init() {
	c := runtime.NumCPU()
	runtime.GOMAXPROCS(c)

	var err error
	loader, err = bqloader.New(bqloader.WithLogLevel("debug"), bqloader.WithConcurrency(c))
	if err != nil {
		panic(err)
	}

	t := handlers.TableGenerator(os.Getenv("BIGQUERY_PROJECT_ID"), os.Getenv("BIGQUERY_DATASET_ID"))
	n := &bqloader.SlackNotifier{
		Token:   os.Getenv("SLACK_TOKEN"),
		Channel: os.Getenv("SLACK_CHANNEL"),
	}

	handlers.MustAddHandlers(context.Background(), loader,
		handlers.SBISumishinNetBankStatement("SBI Bank", `^csv/sbi_bank`, t("sbi_bank"), n),
		handlers.SBISecuritiesGlobalExecutionHistory("SBI Sec", `^csv/sbi_securities`, t("sbi_sec"), n),
		handlers.SMBCCardStatement("SMBC Card", `^csv/\d+\.csv$`, t("smbc_card"), n),
	)
}

// BQLoad is the entrypoint for Cloud Functions.
func BQLoad(ctx context.Context, e bqloader.Event) error {
	return loader.Handle(ctx, e)
}
