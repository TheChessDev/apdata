package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "apdata",
	Short: "Clone data from cloud envs to local",
}

func Execute() {
	cobra.CheckErr(rootCmd.Execute())
}
