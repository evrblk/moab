package commands

import (
	"github.com/spf13/cobra"
)

// runCmd represents the base command for running Moab in different clustered and non-clustered modes
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run Moab",
	Long:  "",
}

func init() {
	rootCmd.AddCommand(runCmd)
}
