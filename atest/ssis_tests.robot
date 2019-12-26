*** Settings ***

Library             SSISLibrary
Variables           ${CONFIG_DIR}/adventureworks.py

Test Setup          connect         ${ssis_connection_string}
Test Teardown       disconnect
Default Tags        SSIS

*** Test Cases ***
Poke around
    ${config}=          list catalog properties
    log                 ${config}
    ${folders}=         list folders
    log                 ${folders}
    ${projects}=        list all projects
    log                 ${projects}

*** Keywords ***
