package ipdb

import (
	"github.com/spf13/cobra"
	"github.com/rs/zerolog"
)

var AppName = "appname"
var AppLogger zerolog.Logger
var AppCmd *cobra.Command
