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