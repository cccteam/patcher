# Changelog

## [0.6.0](https://github.com/cccteam/patcher/compare/v0.5.1...v0.6.0) (2024-10-23)


### ⚠ BREAKING CHANGES

* Changed types `Event` -> `Mutation` and `DeleteEvent` -> `DeleteMutation` ([#26](https://github.com/cccteam/patcher/issues/26))

### Code Refactoring

* Changed types `Event` -&gt; `Mutation` and `DeleteEvent` -&gt; `DeleteMutation` ([#26](https://github.com/cccteam/patcher/issues/26)) ([763b954](https://github.com/cccteam/patcher/commit/763b95415b1d859631d26b09ed8f9a6a94707a94))
* Refactor event types to migration types ([#26](https://github.com/cccteam/patcher/issues/26)) ([763b954](https://github.com/cccteam/patcher/commit/763b95415b1d859631d26b09ed8f9a6a94707a94))

## [0.5.1](https://github.com/cccteam/patcher/compare/v0.5.0...v0.5.1) (2024-10-23)


### Features

* New constructor for loading key map ([#24](https://github.com/cccteam/patcher/issues/24)) ([3a227ca](https://github.com/cccteam/patcher/commit/3a227ca58fe7b9dc54a7b3fa5245561b84eec529))

## [0.5.0](https://github.com/cccteam/patcher/compare/v0.4.0...v0.5.0) (2024-10-14)


### ⚠ BREAKING CHANGES

* Refactor public method names ([#20](https://github.com/cccteam/patcher/issues/20))

### Features

* Add InsertOrUpdate support ([#20](https://github.com/cccteam/patcher/issues/20)) ([a0e1747](https://github.com/cccteam/patcher/commit/a0e17471f80f85347f7265065c3d65e14f0a8211))


### Dependencies

* Go dependency update ([#22](https://github.com/cccteam/patcher/issues/22)) ([239005f](https://github.com/cccteam/patcher/commit/239005f8990f5a02b2b93dd0be085e641d087950))


### Code Refactoring

* Refactor public method names ([#20](https://github.com/cccteam/patcher/issues/20)) ([a0e1747](https://github.com/cccteam/patcher/commit/a0e17471f80f85347f7265065c3d65e14f0a8211))

## [0.4.0](https://github.com/cccteam/patcher/compare/v0.3.0...v0.4.0) (2024-10-11)


### ⚠ BREAKING CHANGES

* Buffer mutations internaly instead of returning them ([#18](https://github.com/cccteam/patcher/issues/18))

### Features

* Buffer mutations internaly instead of returning them ([#18](https://github.com/cccteam/patcher/issues/18)) ([724e50e](https://github.com/cccteam/patcher/commit/724e50e6d3023ab66ceb1720e639a4d21760de71))

## [0.3.0](https://github.com/cccteam/patcher/compare/v0.2.0...v0.3.0) (2024-10-08)


### ⚠ BREAKING CHANGES

* Remove PatchSet from Delete mutations ([#16](https://github.com/cccteam/patcher/issues/16))

### Bug Fixes

* Remove PatchSet from Delete mutations ([#16](https://github.com/cccteam/patcher/issues/16)) ([84d941e](https://github.com/cccteam/patcher/commit/84d941e3d0b1abba3960226f591978ce4ad3c0e8))

## [0.2.0](https://github.com/cccteam/patcher/compare/v0.1.0...v0.2.0) (2024-10-04)


### ⚠ BREAKING CHANGES

* Switch TableName type to Resource ([#12](https://github.com/cccteam/patcher/issues/12))

### Code Refactoring

* Switch TableName type to Resource ([#12](https://github.com/cccteam/patcher/issues/12)) ([1b72ff3](https://github.com/cccteam/patcher/commit/1b72ff31508040701e2d9e9151e99a3259250d1f))

## [0.1.0](https://github.com/cccteam/patcher/compare/v0.0.4...v0.1.0) (2024-10-02)


### ⚠ BREAKING CHANGES

* Refactor to use new types from accesstypes ([#10](https://github.com/cccteam/patcher/issues/10))

### Code Refactoring

* Refactor to use new types from accesstypes ([#10](https://github.com/cccteam/patcher/issues/10)) ([7c11381](https://github.com/cccteam/patcher/commit/7c11381205692064ec4275ecf7d80fe13e5e7906))

## [0.0.4](https://github.com/cccteam/patcher/compare/v0.0.3...v0.0.4) (2024-09-23)


### Features

* Implement ViewableColumns() to return the database struct tags for the fields that the user has access to view ([#7](https://github.com/cccteam/patcher/issues/7)) ([de65964](https://github.com/cccteam/patcher/commit/de659642410781c3ce315fbba786d6bf583f212b))


### Bug Fixes

* Fix bug in TextMarshaler support to handle pointers ([#7](https://github.com/cccteam/patcher/issues/7)) ([de65964](https://github.com/cccteam/patcher/commit/de659642410781c3ce315fbba786d6bf583f212b))

## [0.0.3](https://github.com/cccteam/patcher/compare/v0.0.2...v0.0.3) (2024-09-17)


### Bug Fixes

* Fix bug in KeySet ([#5](https://github.com/cccteam/patcher/issues/5)) ([1b0d4d9](https://github.com/cccteam/patcher/commit/1b0d4d95571c52eeff4828a285200d83ee5c301c))
* Fix bug in Query Builder ([#5](https://github.com/cccteam/patcher/issues/5)) ([1b0d4d9](https://github.com/cccteam/patcher/commit/1b0d4d95571c52eeff4828a285200d83ee5c301c))

## [0.0.2](https://github.com/cccteam/patcher/compare/v0.0.1...v0.0.2) (2024-09-17)


### Features

* Initial release of patcher ([#1](https://github.com/cccteam/patcher/issues/1)) ([3235e4e](https://github.com/cccteam/patcher/commit/3235e4ec8a68d37bac7ad7d18a4f79dee0dc4107))
