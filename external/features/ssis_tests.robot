*** Settings ***

Library             SSISLibrary
Variables           ${CONFIG_DIR}/adventureworks.py

Test Setup          Connect to SSIS DB
Test Teardown       disconnect
Default Tags        SSIS

*** Test Cases ***
Poke around
    ${config}=          list catalog properties
    log                 ${config}
    ${folders}=         list folders
    log                 ${folders}

*** Keywords ***
Connect to SSIS DB
    connect         ${ssis_connection_string}