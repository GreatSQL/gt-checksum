package schemacompat

import (
	"fmt"
	"sort"
	"strings"
)

type ConstraintRepairLevel string

const (
	ConstraintRepairLevelAdvisoryOnly ConstraintRepairLevel = "advisory-only"
	ConstraintRepairLevelManualReview ConstraintRepairLevel = "manual-review"
)

type ConstraintRepairSuggestion struct {
	ConstraintName string
	Kind           string
	Level          ConstraintRepairLevel
	Reason         string
	Statements     []string
}

// MariaDB-only temporal and sequence features are intentionally exported as
// advisory items because there is no safe automatic rewrite to native MySQL.
func BuildMariaDBUnsupportedFeatureSuggestions(destSchema, destTable string, features []UnsupportedDDLFeature) []ConstraintRepairSuggestion {
	suggestions := make([]ConstraintRepairSuggestion, 0, len(features))
	for _, feature := range features {
		reason := strings.TrimSpace(feature.Reason)
		if reason == "" {
			reason = fmt.Sprintf("unsupported MariaDB feature %s requires manual redesign before applying DDL on `%s`.`%s`", feature.Kind, destSchema, destTable)
		}
		suggestions = append(suggestions, ConstraintRepairSuggestion{
			ConstraintName: feature.ObjectName,
			Kind:           feature.Kind,
			Level:          ConstraintRepairLevelManualReview,
			Reason:         reason,
		})
	}
	return suggestions
}

func BuildMariaDBSequenceObjectSuggestions(schema string, sequenceNames []string) []ConstraintRepairSuggestion {
	suggestions := make([]ConstraintRepairSuggestion, 0, len(sequenceNames))
	for _, sequenceName := range sequenceNames {
		if strings.TrimSpace(sequenceName) == "" {
			continue
		}
		suggestions = append(suggestions, ConstraintRepairSuggestion{
			ConstraintName: sequenceName,
			Kind:           "SEQUENCE",
			Level:          ConstraintRepairLevelManualReview,
			Reason: fmt.Sprintf(
				"MariaDB sequence `%s`.`%s` has no native MySQL equivalent; automatic migration is disabled",
				schema,
				sequenceName,
			),
		})
	}
	return suggestions
}

func BuildMariaDBJSONDowngradeSuggestions(destSchema, destTable string, columnNames []string, jsonTargetType string) []ConstraintRepairSuggestion {
	suggestions := make([]ConstraintRepairSuggestion, 0, len(columnNames))
	targetType := strings.ToUpper(strings.TrimSpace(jsonTargetType))
	if targetType == "" {
		targetType = "JSON"
	}
	for _, columnName := range columnNames {
		if strings.TrimSpace(columnName) == "" {
			continue
		}
		suggestions = append(suggestions, ConstraintRepairSuggestion{
			ConstraintName: columnName,
			Kind:           "COLUMN TYPE",
			Level:          ConstraintRepairLevelAdvisoryOnly,
			Reason: fmt.Sprintf(
				"MariaDB JSON alias column `%s`.`%s`.`%s` is configured to downgrade to %s on MySQL target; semantic behavior may differ from native MySQL JSON",
				destSchema,
				destTable,
				columnName,
				targetType,
			),
		})
	}
	return suggestions
}

func BuildCheckConstraintRepairSuggestions(destSchema, destTable string, source, target []CanonicalConstraint, decision CompatibilityDecision) []ConstraintRepairSuggestion {
	suggestions := make([]ConstraintRepairSuggestion, 0)

	sourceByName := make(map[string]CanonicalConstraint, len(source))
	targetByName := make(map[string]CanonicalConstraint, len(target))
	unionNames := make([]string, 0, len(source)+len(target))
	seen := make(map[string]struct{}, len(source)+len(target))

	for _, item := range source {
		key := strings.ToUpper(item.Name)
		sourceByName[key] = item
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			unionNames = append(unionNames, key)
		}
	}
	for _, item := range target {
		key := strings.ToUpper(item.Name)
		targetByName[key] = item
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			unionNames = append(unionNames, key)
		}
	}
	sort.Strings(unionNames)

	for _, key := range unionNames {
		sourceConstraint, sourceExists := sourceByName[key]
		targetConstraint, targetExists := targetByName[key]

		switch {
		case sourceExists && !targetExists:
			suggestions = append(suggestions, ConstraintRepairSuggestion{
				ConstraintName: sourceConstraint.Name,
				Kind:           "CHECK",
				Level:          ConstraintRepairLevelManualReview,
				Reason:         "target is missing a CHECK constraint that exists on the source side",
				Statements:     []string{renderMySQLAddCheckConstraint(destSchema, destTable, sourceConstraint)},
			})
		case !sourceExists && targetExists:
			dropSQL, canDrop := renderMySQLDropCheckConstraint(destSchema, destTable, targetConstraint)
			statements := []string{}
			reason := "target has an extra CHECK constraint that does not exist on the source side"
			if canDrop {
				statements = append(statements, dropSQL)
			} else {
				reason += "; no direct DROP SQL was generated because the constraint name is synthetic"
			}
			suggestions = append(suggestions, ConstraintRepairSuggestion{
				ConstraintName: targetConstraint.Name,
				Kind:           "CHECK",
				Level:          ConstraintRepairLevelManualReview,
				Reason:         reason,
				Statements:     statements,
			})
		case sourceExists && targetExists && sourceConstraint.NormalizedDefinition != targetConstraint.NormalizedDefinition:
			statements := []string{}
			if dropSQL, canDrop := renderMySQLDropCheckConstraint(destSchema, destTable, targetConstraint); canDrop {
				statements = append(statements, dropSQL)
			}
			statements = append(statements, renderMySQLAddCheckConstraint(destSchema, destTable, sourceConstraint))
			suggestions = append(suggestions, ConstraintRepairSuggestion{
				ConstraintName: sourceConstraint.Name,
				Kind:           "CHECK",
				Level:          ConstraintRepairLevelManualReview,
				Reason:         "CHECK constraint definitions differ between source and target",
				Statements:     statements,
			})
		}
	}

	if len(suggestions) == 0 && decision.State == CompatibilityWarnOnly {
		suggestions = append(suggestions, ConstraintRepairSuggestion{
			Kind:   "CHECK",
			Level:  ConstraintRepairLevelAdvisoryOnly,
			Reason: decision.Reason,
		})
	}

	return suggestions
}

func BuildForeignKeyRepairSuggestions(destSchema, destTable string, source, target []CanonicalConstraint, schemaMappings map[string]string) []ConstraintRepairSuggestion {
	suggestions := make([]ConstraintRepairSuggestion, 0)

	sourceByName := make(map[string]CanonicalConstraint, len(source))
	targetByName := make(map[string]CanonicalConstraint, len(target))
	unionNames := make([]string, 0, len(source)+len(target))
	seen := make(map[string]struct{}, len(source)+len(target))

	for _, item := range source {
		key := strings.ToUpper(item.Name)
		sourceByName[key] = item
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			unionNames = append(unionNames, key)
		}
	}
	for _, item := range target {
		key := strings.ToUpper(item.Name)
		targetByName[key] = item
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			unionNames = append(unionNames, key)
		}
	}
	sort.Strings(unionNames)

	for _, key := range unionNames {
		sourceConstraint, sourceExists := sourceByName[key]
		targetConstraint, targetExists := targetByName[key]

		switch {
		case sourceExists && !targetExists:
			suggestions = append(suggestions, ConstraintRepairSuggestion{
				ConstraintName: sourceConstraint.Name,
				Kind:           "FOREIGN KEY",
				Level:          ConstraintRepairLevelManualReview,
				Reason:         "target is missing a foreign key that exists on the source side",
				Statements:     []string{renderMySQLAddForeignKey(destSchema, destTable, sourceConstraint, schemaMappings)},
			})
		case !sourceExists && targetExists:
			suggestions = append(suggestions, ConstraintRepairSuggestion{
				ConstraintName: targetConstraint.Name,
				Kind:           "FOREIGN KEY",
				Level:          ConstraintRepairLevelManualReview,
				Reason:         "target has an extra foreign key that does not exist on the source side",
				Statements: []string{
					fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP FOREIGN KEY `%s`", destSchema, destTable, targetConstraint.Name),
				},
			})
		case sourceExists && targetExists && sourceConstraint.NormalizedDefinition != targetConstraint.NormalizedDefinition:
			suggestions = append(suggestions, ConstraintRepairSuggestion{
				ConstraintName: sourceConstraint.Name,
				Kind:           "FOREIGN KEY",
				Level:          ConstraintRepairLevelManualReview,
				Reason:         "foreign key definitions differ between source and target",
				Statements: []string{
					fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP FOREIGN KEY `%s`", destSchema, destTable, targetConstraint.Name),
					renderMySQLAddForeignKey(destSchema, destTable, sourceConstraint, schemaMappings),
				},
			})
		}
	}

	return suggestions
}

func BuildStrictForeignKeyRepairSuggestions(strictIssues []CanonicalConstraint, schemaMappings map[string]string) []ConstraintRepairSuggestion {
	suggestions := make([]ConstraintRepairSuggestion, 0, len(strictIssues))
	for _, issue := range strictIssues {
		referencedSchema := mapConstraintSchema(issue.ReferencedSchema, schemaMappings)
		suggestions = append(suggestions, ConstraintRepairSuggestion{
			ConstraintName: issue.Name,
			Kind:           "FOREIGN KEY SUPPORTING INDEX",
			Level:          ConstraintRepairLevelManualReview,
			Reason: fmt.Sprintf(
				"target MySQL 8.4 requires an exact UNIQUE or PRIMARY KEY on %s.%s(%s) before the foreign key can be recreated",
				referencedSchema,
				issue.ReferencedTable,
				strings.Join(issue.ReferencedColumns, ", "),
			),
			Statements: []string{
				renderMySQLAddSupportingUniqueIndex(issue, schemaMappings),
			},
		})
	}
	return suggestions
}

func renderMySQLAddCheckConstraint(destSchema, destTable string, constraint CanonicalConstraint) string {
	definition := strings.TrimSpace(constraint.NormalizedDefinition)
	if definition == "" {
		definition = strings.TrimSpace(constraint.Definition)
	}
	if isSyntheticCheckConstraintName(constraint.Name) {
		return fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD CHECK %s", destSchema, destTable, definition)
	}
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD CONSTRAINT `%s` CHECK %s", destSchema, destTable, constraint.Name, definition)
}

func renderMySQLDropCheckConstraint(destSchema, destTable string, constraint CanonicalConstraint) (string, bool) {
	if isSyntheticCheckConstraintName(constraint.Name) {
		return "", false
	}
	return fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP CHECK `%s`", destSchema, destTable, constraint.Name), true
}

func renderMySQLAddForeignKey(destSchema, destTable string, constraint CanonicalConstraint, schemaMappings map[string]string) string {
	referencedSchema := mapConstraintSchema(constraint.ReferencedSchema, schemaMappings)
	statement := fmt.Sprintf(
		"ALTER TABLE `%s`.`%s` ADD CONSTRAINT `%s` FOREIGN KEY (%s) REFERENCES `%s`.`%s` (%s)",
		destSchema,
		destTable,
		constraint.Name,
		quoteIdentifiers(constraint.Columns),
		referencedSchema,
		constraint.ReferencedTable,
		quoteIdentifiers(constraint.ReferencedColumns),
	)
	if constraint.DeleteRule != "" && constraint.DeleteRule != "NO ACTION" && constraint.DeleteRule != "RESTRICT" {
		statement += " ON DELETE " + constraint.DeleteRule
	}
	if constraint.UpdateRule != "" && constraint.UpdateRule != "NO ACTION" && constraint.UpdateRule != "RESTRICT" {
		statement += " ON UPDATE " + constraint.UpdateRule
	}
	return statement
}

func renderMySQLAddSupportingUniqueIndex(constraint CanonicalConstraint, schemaMappings map[string]string) string {
	referencedSchema := mapConstraintSchema(constraint.ReferencedSchema, schemaMappings)
	indexName := buildSupportingUniqueIndexName(constraint.Name)
	return fmt.Sprintf(
		"ALTER TABLE `%s`.`%s` ADD UNIQUE INDEX `%s` (%s)",
		referencedSchema,
		constraint.ReferencedTable,
		indexName,
		quoteIdentifiers(constraint.ReferencedColumns),
	)
}

func quoteIdentifiers(items []string) string {
	quoted := make([]string, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item) == "" {
			continue
		}
		quoted = append(quoted, fmt.Sprintf("`%s`", item))
	}
	return strings.Join(quoted, ", ")
}

func mapConstraintSchema(schema string, schemaMappings map[string]string) string {
	if mapped, ok := schemaMappings[schema]; ok && strings.TrimSpace(mapped) != "" {
		return mapped
	}
	return schema
}

func isSyntheticCheckConstraintName(name string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(name)), "CHECK_")
}

func buildSupportingUniqueIndexName(constraintName string) string {
	var builder strings.Builder
	builder.WriteString("uk_gtc_")
	for _, ch := range strings.ToLower(constraintName) {
		switch {
		case ch >= 'a' && ch <= 'z':
			builder.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			builder.WriteRune(ch)
		default:
			builder.WriteByte('_')
		}
		if builder.Len() >= 64 {
			break
		}
	}

	name := strings.Trim(builder.String(), "_")
	if name == "" {
		name = "uk_gtc_fk_support"
	}
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}
