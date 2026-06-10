package main

import (
	"errors"
	"flag"
	"fmt"
	"gt-checksum/connstr"
	"io"
	"os"
	"strings"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "gt-dsn-crypt:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printUsage(stderr)
		return errors.New("missing command")
	}

	switch args[0] {
	case "gen-key":
		return runGenKey(args[1:], stdout, stderr)
	case "encrypt":
		return runEncrypt(args[1:], stdout, stderr)
	case "decrypt":
		return runDecrypt(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		printUsage(stdout)
		return nil
	default:
		printUsage(stderr)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runGenKey(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("gen-key", stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("gen-key does not accept positional arguments")
	}

	key, err := connstr.GenerateKey()
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, key)
	return nil
}

func runEncrypt(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("encrypt", stderr)
	keyValue := fs.String("key", "", "Base64 encoded 32-byte key; overrides GT_CHECKSUM_DSN_KEY")
	passwordFile := fs.String("password-file", "", "File containing the plaintext password")
	password := fs.String("password", "", "Plaintext password; may be stored in shell history")
	kid := fs.String("kid", "default", "Key id stored in ENC[...] for future rotation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("encrypt does not accept positional arguments")
	}

	plain, err := readPlainPassword(*password, *passwordFile)
	if err != nil {
		return err
	}
	key, err := connstr.LoadKey(*keyValue)
	if err != nil {
		return err
	}
	ciphertext, err := connstr.EncryptPassword(plain, key, *kid)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, ciphertext)
	return nil
}

func runDecrypt(args []string, stdout, stderr io.Writer) error {
	fs := newFlagSet("decrypt", stderr)
	keyValue := fs.String("key", "", "Base64 encoded 32-byte key; overrides GT_CHECKSUM_DSN_KEY")
	ciphertext := fs.String("ciphertext", "", "ENC[...] ciphertext to decrypt")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("decrypt does not accept positional arguments")
	}
	if strings.TrimSpace(*ciphertext) == "" {
		return fmt.Errorf("--ciphertext is required")
	}

	key, err := connstr.LoadKey(*keyValue)
	if err != nil {
		return err
	}
	plain, err := connstr.DecryptPassword(*ciphertext, key)
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, plain)
	return nil
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

func readPlainPassword(password, passwordFile string) (string, error) {
	hasPassword := password != ""
	hasPasswordFile := strings.TrimSpace(passwordFile) != ""
	if hasPassword == hasPasswordFile {
		return "", fmt.Errorf("specify exactly one of --password or --password-file")
	}
	if hasPassword {
		return password, nil
	}

	data, err := os.ReadFile(passwordFile)
	if err != nil {
		return "", fmt.Errorf("read password file: %w", err)
	}
	return strings.TrimRight(string(data), "\r\n"), nil
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  gt-dsn-crypt gen-key")
	fmt.Fprintln(w, "  gt-dsn-crypt encrypt --password-file ./password.txt [--key <base64-key>] [--kid default]")
	fmt.Fprintln(w, "  gt-dsn-crypt encrypt --password '<plain-password>' [--key <base64-key>] [--kid default]")
	fmt.Fprintln(w, "  gt-dsn-crypt decrypt --ciphertext 'ENC[...]' [--key <base64-key>]")
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "If --key is omitted, GT_CHECKSUM_DSN_KEY is used. Key files are intentionally not supported.")
}
