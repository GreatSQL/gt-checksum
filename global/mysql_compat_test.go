package global

import "testing"

// ---------------------------------------------------------------------------
// IsSupportedMariaDBSeries
// ---------------------------------------------------------------------------

func TestIsSupportedMariaDBSeries_SupportedVersions(t *testing.T) {
	for _, series := range []string{"10.5", "10.6", "10.11", "11.4", "11.5", "12.3"} {
		if !IsSupportedMariaDBSeries(series) {
			t.Errorf("expected %q to be supported", series)
		}
	}
}

func TestIsSupportedMariaDBSeries_UnsupportedVersions(t *testing.T) {
	for _, series := range []string{"10.4", "10.9", "11.0", "11.3", "13.0", "5.7", "8.0"} {
		if IsSupportedMariaDBSeries(series) {
			t.Errorf("expected %q to be unsupported", series)
		}
	}
}

// ---------------------------------------------------------------------------
// validateMariaDBToMariaDBPolicy
// ---------------------------------------------------------------------------

func TestValidateMariaDBToMariaDBPolicy_SameVersion(t *testing.T) {
	src, _ := ParseMySQLVersion("10.6.15-MariaDB")
	dst, _ := ParseMySQLVersion("10.6.18-MariaDB")
	if err := validateMariaDBToMariaDBPolicy(src, dst); err != nil {
		t.Errorf("unexpected error for same-series pair: %v", err)
	}
}

func TestValidateMariaDBToMariaDBPolicy_Upgrade(t *testing.T) {
	src, _ := ParseMySQLVersion("10.6.15-MariaDB")
	dst, _ := ParseMySQLVersion("11.4.2-MariaDB")
	if err := validateMariaDBToMariaDBPolicy(src, dst); err != nil {
		t.Errorf("unexpected error for valid upgrade pair: %v", err)
	}
}

func TestValidateMariaDBToMariaDBPolicy_Downgrade(t *testing.T) {
	src, _ := ParseMySQLVersion("11.4.2-MariaDB")
	dst, _ := ParseMySQLVersion("10.6.15-MariaDB")
	if err := validateMariaDBToMariaDBPolicy(src, dst); err == nil {
		t.Error("expected error for downgrade pair, got nil")
	}
}

func TestValidateMariaDBToMariaDBPolicy_UnsupportedSourceSeries(t *testing.T) {
	src, _ := ParseMySQLVersion("10.4.30-MariaDB")
	dst, _ := ParseMySQLVersion("10.6.15-MariaDB")
	if err := validateMariaDBToMariaDBPolicy(src, dst); err == nil {
		t.Error("expected error for unsupported source series, got nil")
	}
}

func TestValidateMariaDBToMariaDBPolicy_UnsupportedDestSeries(t *testing.T) {
	src, _ := ParseMySQLVersion("10.6.15-MariaDB")
	dst, _ := ParseMySQLVersion("10.9.4-MariaDB")
	if err := validateMariaDBToMariaDBPolicy(src, dst); err == nil {
		t.Error("expected error for unsupported dest series, got nil")
	}
}

// ---------------------------------------------------------------------------
// ValidateMySQLCompatibilityPolicy — MariaDB→MariaDB cases
// ---------------------------------------------------------------------------

func TestValidateMySQLCompatibilityPolicy_MariaDBToMariaDB_AllCheckObjects(t *testing.T) {
	src, _ := ParseMySQLVersion("10.6.15-MariaDB")
	dst, _ := ParseMySQLVersion("10.11.6-MariaDB")
	for _, co := range []string{"data", "struct", "routine", "trigger"} {
		if err := ValidateMySQLCompatibilityPolicy(src, dst, co); err != nil {
			t.Errorf("unexpected error for checkObject=%q: %v", co, err)
		}
	}
}

func TestValidateMySQLCompatibilityPolicy_MariaDBToMariaDB_SameSeries(t *testing.T) {
	src, _ := ParseMySQLVersion("11.4.2-MariaDB")
	dst, _ := ParseMySQLVersion("11.4.5-MariaDB")
	if err := ValidateMySQLCompatibilityPolicy(src, dst, "data"); err != nil {
		t.Errorf("unexpected error for same-series MariaDB pair: %v", err)
	}
}

func TestValidateMySQLCompatibilityPolicy_MariaDBToMariaDB_Downgrade(t *testing.T) {
	src, _ := ParseMySQLVersion("11.4.2-MariaDB")
	dst, _ := ParseMySQLVersion("10.6.15-MariaDB")
	if err := ValidateMySQLCompatibilityPolicy(src, dst, "data"); err == nil {
		t.Error("expected error for MariaDB downgrade, got nil")
	}
}

func TestValidateMySQLCompatibilityPolicy_MariaDBToMariaDB_UnsupportedSeries(t *testing.T) {
	src, _ := ParseMySQLVersion("10.4.30-MariaDB")
	dst, _ := ParseMySQLVersion("10.6.15-MariaDB")
	if err := ValidateMySQLCompatibilityPolicy(src, dst, "data"); err == nil {
		t.Error("expected error for unsupported MariaDB source series, got nil")
	}
}

func TestValidateMySQLCompatibilityPolicy_MariaDBToMariaDB_UnsupportedCheckObject(t *testing.T) {
	src, _ := ParseMySQLVersion("10.6.15-MariaDB")
	dst, _ := ParseMySQLVersion("10.11.6-MariaDB")
	if err := ValidateMySQLCompatibilityPolicy(src, dst, "count"); err == nil {
		t.Error("expected error for unsupported checkObject, got nil")
	}
}

// ---------------------------------------------------------------------------
// Regression: existing policies must remain unchanged
// ---------------------------------------------------------------------------

func TestValidateMySQLCompatibilityPolicy_MySQLToMySQL(t *testing.T) {
	src, _ := ParseMySQLVersion("5.7.38")
	dst, _ := ParseMySQLVersion("8.0.35")
	if err := ValidateMySQLCompatibilityPolicy(src, dst, "data"); err != nil {
		t.Errorf("unexpected error for MySQL 5.7->8.0: %v", err)
	}
}

func TestValidateMySQLCompatibilityPolicy_MariaDBToMySQL(t *testing.T) {
	src, _ := ParseMySQLVersion("10.6.15-MariaDB")
	dst, _ := ParseMySQLVersion("8.0.35")
	if err := ValidateMySQLCompatibilityPolicy(src, dst, "struct"); err != nil {
		t.Errorf("unexpected error for MariaDB->MySQL 8.0: %v", err)
	}
}

func TestValidateMySQLCompatibilityPolicy_MySQLToMariaDB_StillBlocked(t *testing.T) {
	src, _ := ParseMySQLVersion("8.0.35")
	dst, _ := ParseMySQLVersion("10.6.15-MariaDB")
	if err := ValidateMySQLCompatibilityPolicy(src, dst, "data"); err == nil {
		t.Error("expected error for MySQL->MariaDB (still unsupported), got nil")
	}
}
