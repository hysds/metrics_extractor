<!-- Header block for project -->
<hr>

<div align="center">

<!-- ☝️ Replace with your logo (if applicable) via ![](https://uri-to-your-logo-image) ☝️ -->
<!-- ☝️ If you see logo rendering errors, make sure you're not using indentation, or try an HTML IMG tag -->

<h1 align="center">HySDS Metrics Extractor</h1>
<!-- ☝️ Replace with your repo name ☝️ -->

</div>

<pre align="center">Tool to extract HySDS metrics from ES to export out reports.</pre>
<!-- ☝️ Replace with a single sentence describing the purpose of your repo / proj ☝️ -->

<!-- Header block for project -->

Tools for extracting metrics out of captured runtime performance of jobs from HySDS metrics in Elasticsearch/OpenSearch. These reports can be used by ops and can also be used as input to other cost-production models for anlaysis.
<!-- ☝️ Replace with a more detailed description of your repository, including why it was made and whom its intended for.  ☝️ -->

Metrics extractions are important to get actuals from production, which can then be used as input for modeling estimations based on actuals.
<!-- example links>
[Website]([INSERT WEBSITE LINK HERE]) | [Docs/Wiki]([INSERT DOCS/WIKI SITE LINK HERE]) | [Discussion Board]([INSERT DISCUSSION BOARD LINK HERE]) | [Issue Tracker]([INSERT ISSUE TRACKER LINK HERE])
-->

## Features

* Extracts actual runtime metrics from a HySDS venue.
* Extracts all metrics enumerations of each job type and its set of compute instance types.
* Metrics extracts uses ES' built-in aggregrations API to compute statistics on the ES server side.

<!-- ☝️ Replace with a bullet-point list of your features ☝️ -->

## Contents

* [Quick Start](#quick-start)
* [Changelog](#changelog)
* [FAQ](#frequently-asked-questions-faq)
* [Contributing Guide](#contributing)
* [License](#license)
* [Support](#support)

## Quick Start

This guide provides a quick way to get started with our project.

### Requirements

* Python 3.8+
* access to ES endpoint containing the logstash indices of a HySDS Metrics service.
  
<!-- ☝️ Replace with a numbered list of your requirements, including hardware if applicable ☝️ -->

### Setup Instructions

    $ git clone https://github.com/hysds/metrics_extractor/

The main tool is:

    metrics_extractor/metrics_extractor/hysds_metrics_es_extractor.py

<!-- ☝️ Replace with a numbered list of how to set up your software prior to running ☝️ -->

### Run Instructions

The hysds_metrics_es_extractor.py tool requires a HySDS Metrics ES url endpoint, and a temporal range to query against.
The ES URL endpoint typically has the form:

- https://my_venue/__mozart_es__/logstash-*/_search
- https://my_venue/__metrics_es__/logstash-*/_search

The temporal range can be provided in one of two ways:

1. __--days_back__=NN , where NN is the number of days back to search starting from "now".
2. __--time_start__=20240101T000000Z --time_end=20240313T000000Z , where the timestamps extents are in UTC format with the trailing "Z".

Verbosity options:

1. __--verbose__
2. __--debug__

<!-- ☝️ Replace with a numbered list of your run instructions, including expected results ☝️ -->

### Usage Examples

Quick start examples:

    $ hysds_metrics_es_extractor.py --verbose --es_url="https://my_pcm_venue/mozart_es/logstash-*/_search" --days_back=21
    $ hysds_metrics_es_extractor.py --debug --es_url="https://my_pcm_venue/metrics_es/logstash-*/_search"  --time_start=20240101T000000Z --time_end=20240313T000000Z

This will produce an output csv report of the file name

    "job_metrics {hostname} {start}-{end} spanning {duration_days} days.csv".

<!-- ☝️ Replace with a list of your usage examples, including screenshots if possible, and link to external documentation for details ☝️ -->

## Changelog

See our [CHANGELOG.md](CHANGELOG.md) for a history of our changes.

<!-- ☝️ Replace with links to your changelog and releases page ☝️ -->

## Frequently Asked Questions (FAQ)

<!-- example link to FAQ PAGE>
Questions about our project? Please see our: [FAQ]([INSERT LINK TO FAQ / DISCUSSION BOARD])
-->

<!-- example FAQ inline format>
1. Question 1
   - Answer to question 1
2. Question 2
   - Answer to question 2
-->

<!-- example FAQ inline with no questions yet>
No questions yet. Propose a question to be added here by reaching out to our contributors! See support section below.
-->

<!-- ☝️ Replace with a list of frequently asked questions from your project, or post a link to your FAQ on a discussion board ☝️ -->

## Contributing

<!-- example link to CONTRIBUTING.md>
Interested in contributing to our project? Please see our: [CONTRIBUTING.md](CONTRIBUTING.md)
-->

<!-- example inline contributing guide>
1. Create an GitHub issue ticket describing what changes you need (e.g. issue-1)
2. [Fork]([INSERT LINK TO YOUR REPO FORK PAGE HERE, e.g. https://github.com/my_org/my_repo/fork]) this repo
3. Make your modifications in your own fork
4. Make a pull-request in this repo with the code in your fork and tag the repo owner / largest contributor as a reviewer

**Working on your first pull request?** See guide: [How to Contribute to an Open Source Project on GitHub](https://kcd.im/pull-request)
-->


<!-- example link to CODE_OF_CONDUCT.md>
For guidance on how to interact with our team, please see our code of conduct located at: [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
-->

<!-- ☝️ Replace with a text describing how people may contribute to your project, or link to your contribution guide directly ☝️ -->



<!-- example link to GOVERNANCE.md>
For guidance on our governance approach, including decision-making process and our various roles, please see our governance model at: [GOVERNANCE.md](GOVERNANCE.md)
-->

## License

See our: [LICENSE](LICENSE)
<!-- ☝️ Replace with the text of your copyright and license, or directly link to your license file ☝️ -->

## Support


<!-- example list of contacts>
Key points of contact are: [@github-user-1]([INSERT LINK TO GITHUB PROFILE]) [@github-user-2]([INSERT LINK TO GITHUB PROFILE])
-->

<!-- ☝️ Replace with the key individuals who should be contacted for questions ☝️ -->
