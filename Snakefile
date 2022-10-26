# See https://lachlandeer.github.io/snakemake-econ-r-tutorial/automating-list-construction-for-wildcard-expansion.html
# Constants
TOOLSDIR = "../OpenParliamentTV-Tools/optv/parliaments/DE"
MEDIADIR = "original/media"
PROCEEDINGSDIR = "original/proceedings"
MERGEDDIR = "cache/merged"
ALIGNEDDIR = "cache/aligned"
NERDIR = "cache/ner"
PROCESSEDDIR = "processed"

SESSIONS = glob_wildcards("original/media/{session,20[0-9]+}-media.json").session

target_files = expand("processed/{session}-session.json", session=SESSIONS)

configfile: "config.yml"

report: "report/workflow.rst"

rule all:
    input:
        target_files,

rule download_media:
    output:
        "original/media/{session}-media.json",
    shell:
        # Fetch media files from period
        """{TOOLSDIR}/scraper/update_media.py --from-period 20 {MEDIADIR} --save-raw-data"""

rule download_proceedings:
    output:
        "original/proceedings/{session}-data.xml",
    shell:
        """{TOOLSDIR}/scraper/fetch_proceedings.py {PROCEEDINGSDIR}"""

rule parse_proceedings:
     input:
        "original/proceedings/{session}-data.xml"
     output:
        "original/proceedings/{session}-data.json"
     shell:
        "{TOOLSDIR}/parsers/proceedings2json.py --include-nas --output {PROCEEDINGSDIR} {input[0]}"

#rule parse_media:
#     input:
#        raw="original/media/raw-{session}-media.json"
#     output:
#        media="original/media/{session}-media.json",
#     shell:
#        "{TOOLSDIR}/parsers/media2json.py {input.raw} > {output.media}"

rule merge:
     input:
        m="original/media/{session}-media.json",
        p="original/proceedings/{session}-data.json"
     output:
        "cache/merged/{session}-merged.json"
     shell:
        """{TOOLSDIR}/merger/merge_session.py --include-all-proceedings --second-stage-matching --advanced-rematch --output="cache/merged" {input.p} {input.m}"""

rule align:
     input:
        "cache/merged/{session}-merged.json"
     output:
        "cache/aligned/{session}-aligned.json"
     shell:
        """{TOOLSDIR}/aligner/align_sentences.py --cache-dir="cache/audio" {input[0]} {output[0]}"""

rule entities:
     input:
        "cache/aligned/{session}-aligned.json"
     output:
        "cache/ner/{session}-ner.json"
     shell:
        """{TOOLSDIR}/ner/ner.py {input[0]} {output[0]}"""

rule finalize:
     input:
        "cache/ner/{session}-ner.json"
     output:
        "processed/{session}-session.json"
     shell:
        "cp {input[0]} {output}"

#forcemerge:
#   $(BASEDIR)/merger/merge_session.py --include-all-proceedings --second-stage-matching --advanced-rematch --output="$(MERGEDDIR)" "$(PROCEEDINGSDIR#)" "$(MEDIADIR)"
