# packr-source-driver

A [golang-migrate/migrate](https://github.com/golang-migrate/migrate) driver
for [gobuffalo/packr](https://github.com/gobuffalo/packr) based projects.

## Usage

Here's a simple example of how to create a `migrate` instance
with a `packr` based driver:

```golang
import (
	"fmt"
	packrdriver "github.com/fiskeben/packr-source-driver/driver"
	"github.com/gobuffalo/packr"
	"github.com/golang-migrate/migrate"
)

func makeMigrate(connection string) (*migrate.Migrate, error) {
	box := packr.NewBox("./path/to/migration/steps")
	driver, err := packrdriver.WithInstance(box)
	if err != nil {
		return nil, fmt.Errorf("failed to create migration data driver: %v", err)
	}

	return migrate.NewWithSourceInstance("packr", driver, connection)
}

```

## Contribute

PRs are welcome.
