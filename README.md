# tap-idealo-click-report

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap offers streams to download the Idealo click report via the [API](https://business.idealo.com/de/assets/api/click-report.html#_general).

&nbsp;

## Configuration

See [`sample_config.json`](sample_config.json). Overview over the possible config properties:

start_date", "shop_id",  "client_id", "client_password", "site

| Config property   | Required / Default value | Description
| ----------------- | ------------------------ | -----------
| `start_date`      | Yes                      | For streams with replication method INCREMENTAL the start date time to be used
| `shop_id`         | Yes                      | The Idealo shopId the access-token is valid for.
| `client_id`       | Yes                      | Client Id, see [Authentication](https://business.idealo.com/de/assets/api/click-report.html#_authentication)
| `client_password` | Yes                      | Client secret, see [Authentication](https://business.idealo.com/de/assets/api/click-report.html#_authentication)
| `site`            | Yes                      | The site of the report.<br/><br/>Must be one of [IDEALO_DE, IDEALO_AT, IDEALO_UK, IDEALO_FR, IDEALO_IT, IDEALO_ES].
| `user_agent`      | No                       | User agent to be used for HTTP requests

&nbsp;

## Quick Start

1. Install

``` bash
pip install tap-idealo-click-report
```

2. Get the Idealo access credentials, see [Authentication](https://business.idealo.com/de/assets/api/click-report.html#_authentication)

3. Create the Config File

Create a JSON file called `config.json` from the `sample_config.json` file in this
repository.

4. Run the Tap in Discovery Mode

    tap-idealo-click-report -c config.json -d

See the Singer docs on discovery mode
[here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

5. Select streams and properties to sync (use [singer-discover](https://github.com/chrisgoddard/singer-discover) when you are unfamiliar with that)

6. Run the Tap in Sync Mode

    tap-idealo-click-report -c config.json -p catalog.json

---

Copyright &copy; 2021 Horze International GmbH
