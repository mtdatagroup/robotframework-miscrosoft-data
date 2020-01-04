*** Settings ***
Library                   MicrosoftDataLibrary
Variables                 adventureworks.py

*** Variables ***
${TESTDATA_DIRECTORY}     ${CURDIR}${/}..${/}testdata

*** Keywords ***
Connect to Database
    [Arguments]         ${name}             ${alchemy_connection_string}
    Connect             ${name}             ${alchemy_connection_string}

Disconnect from Databases
    Disconnect All

Table Should be Empty
    [Arguments]        ${schema_name}       ${table_name}
    ${is_empty}=       table is empty       ${schema_name}
    ...                                     ${table_name}
    should be true     ${is_empty}