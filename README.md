# qaqc_testing

A repository for the Connect4Cancer QAQC process.

qaqc.R is a script that is used to generate a qc_report (excel), given a query to a BQ dataset (FlatConnect.participants_JP or FlatConnect.biospecimen_JP) and a qc_rules file (excel). The code was written by Daniel Russ in August 2022 and has been maintained by Jake Peters since January 2023.

### Issues:

Use the Issues tab to submit requests for new rules.

When you submit new rules please assign them to a member of the C4C Analytics team and add them to the "QAQC" project so that they can be tracked via the Kanban board.

Please assign recruitment QAQC issues to Madhuri and Jake.

Please assign biospecimen QAQC issues to Kelsey and Jake.

A *how-to guide* for the QAQC rule submission process can be found here: <https://nih.app.box.com/file/1185137275319>

# QC Types (QCTypes) Overview

This document provides a concise summary of the Quality Control (QC) rule types (`qctypes`) used in the R script. Each QC type ensures data integrity by validating specific aspects of the dataset based on predefined criteria.

## Table of Contents

1. [Basic Validations](#basic-validations)
2. [Population Checks](#population-checks)
3. [Data Type Checks](#data-type-checks)
4. [Date and Time Validations](#date-and-time-validations)
5. [String Length Checks](#string-length-checks)
6. [Cross-Variable Validations](#cross-variable-validations)
7. [Matching Values Checks](#matching-values-checks)
8. [Special Validations](#special-validations)

---

## Basic Validations

| **QC Type**        | **Description**                                                                                           |
|--------------------|-----------------------------------------------------------------------------------------------------------|
| `valid`            | Ensures `ConceptID` values are within the specified `ValidValues`.                                       |
| `NA or valid`      | Allows `ConceptID` to be within `ValidValues` or `NA` (missing).                                         |

---

## Population Checks

| **QC Type**          | **Description**                                                                           |
|----------------------|-------------------------------------------------------------------------------------------|
| `is populated`       | Checks that `ConceptID` is not `NA`.                                                      |
| `is not populated`   | Verifies that `ConceptID` is `NA`.                                                        |

---

## Data Type Checks

| **QC Type**          | **Description**                                                                           |
|----------------------|-------------------------------------------------------------------------------------------|
| `isNumeric`          | Confirms that `ConceptID` can be converted to a numeric type.                             |
| `NA or isNumeric`    | Allows `ConceptID` to be numeric or `NA`.                                                |

---

## Date and Time Validations

| **QC Type**                     | **Description**                                                                                     |
|---------------------------------|-----------------------------------------------------------------------------------------------------|
| `valid before date()`           | Ensures `ConceptID` date is before a specified comparison date.                                     |
| `NA or valid before date()`     | Allows `ConceptID` date to be before the comparison date or `NA`.                                   |
| `is 24hr time`                  | Validates that `ConceptID` follows the `HH:MM` 24-hour time format.                                |
| `NA or is 24hr time`            | Allows `ConceptID` to be in 24-hour time format or `NA`.                                          |

---

## String Length Checks

| **QC Type**                             | **Description**                                                                                   |
|-----------------------------------------|---------------------------------------------------------------------------------------------------|
| `has_n_characters`                      | Ensures `ConceptID` string has an exact number of characters.                                     |
| `has_less_than_or_equal_n_characters`    | Checks that `ConceptID` string does not exceed a specified maximum length.                       |
| `NA or has_n_characters`                | Allows `ConceptID` to have the exact length or be `NA`.                                           |
| `NA or has_less_than_or_equal_n_characters` | Allows `ConceptID` to be within the maximum length or be `NA`.                                  |

---

## Cross-Variable Validations

Cross-Variable Validations involve checking the relationship between multiple fields to ensure data consistency based on conditional logic.

| **QC Type**                     | **Description**                                                                                     |
|---------------------------------|-----------------------------------------------------------------------------------------------------|
| `crossValid1`                   | Validates `ConceptID` based on one related variable (`CrossVariableConceptID1`).                   |
| `crossValid2`                   | Extends `crossValid1` by incorporating a second related variable (`CrossVariableConceptID2`).      |
| `crossValid3`                   | Further extends by adding a third related variable (`CrossVariableConceptID3`).                    |
| `crossValid4`                   | Includes a fourth related variable (`CrossVariableConceptID4`) in the validation logic.            |
| `crossValid1 isNumeric`         | Combines `crossValid1` with a numeric validation on `ConceptID`.                                  |
| `crossValid1 is populated`      | Combines `crossValid1` with a check ensuring `ConceptID` is not `NA`.                             |

---

## Matching Values Checks

| **QC Type**                     | **Description**                                                                                     |
|---------------------------------|-----------------------------------------------------------------------------------------------------|
| `match cid values`              | Ensures `ConceptID` matches the value of another specified `ConceptID`.                             |
| `crossvalid match cid values`   | Applies `match cid values` conditionally based on cross-variable logic.                            |
| `NA or match cid values`        | Allows `ConceptID` to match another `ConceptID` or be `NA`.                                       |
| `NA or crossvalid match cid values` | Ensures conditional matching of `ConceptID` or allows `NA`.                                     |

---

## Special Validations

| **QC Type**                             | **Description**                                                                                   |
|-----------------------------------------|---------------------------------------------------------------------------------------------------|
| `crossValid1Date`                       | Ensures `ConceptID` is a valid date when `CrossVariableConceptID1` meets its condition.            |
| `crossValid1NotNA`                      | Requires `ConceptID` to be non-`NA` when `CrossVariableConceptID1` meets its condition.          |
| `crossValid1 equal to char()`            | Checks `ConceptID` string length when `CrossVariableConceptID1` meets its condition.             |
| `crossValid1 equal to or less than char()` | Ensures `ConceptID` string does not exceed a specific length based on `CrossVariableConceptID1`.  |
| `crossValid1 or is 24hr time`            | Validates either the cross-variable condition is met or `ConceptID` is in 24-hour time format.     |
| `NA or crossValid1 is 24hr time`         | Allows `ConceptID` to be in 24-hour time format or `NA`, based on `CrossVariableConceptID1`.      |

---

## Summary

The `qctypes` encompass a wide range of validation rules, including:

- **Basic Checks:** Validating specific values and allowing for missing data.
- **Population Checks:** Ensuring mandatory fields are filled or intentionally left blank.
- **Data Type Enforcement:** Confirming numerical and date/time formats.
- **String Length Constraints:** Maintaining consistency in text fields.
- **Cross-Variable Logic:** Ensuring data consistency based on related fields.
- **Matching Values:** Maintaining consistency across related identifiers.

These QC rules systematically ensure that the dataset adheres to defined standards, maintaining high data quality and reliability.

For further customization or addition of new QC types, extend the existing functions or introduce new validation logic following the established patterns in the script.
