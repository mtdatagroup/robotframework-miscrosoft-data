*** Settings ***

Resource            resources/common.robot
Library             OperatingSystem
Default Tags        SQL Server      Fixture

Test Setup          Connect to Database     AdventureWorks      ${trusted_connection_string}
Test Teardown       Disconnect from Databases

*** Variables ***
${FIXTURE}                      ${TESTDATA_DIRECTORY}/test.csv

*** Test Cases ***
Load data fixtures
    ${fixture_contents}=        Get File                ${FIXTURE}
    Log                         ${fixture_contents}
    truncate table              dbo                     NameAgeTable
    ${row_count}=               table row count         dbo                 NameAgeTable
    should be equal as integers                         ${row_count}        0
    ${row_count}=               load table with CSV     ${FIXTURE}
    ...                                                 dbo
    ...                                                 NameAgeTable
    should not be equal as integers                     ${row_count}        0
    ${records}=                 read_query              SELECT * FROM dbo.NameAgeTable
    Log                         ${records}
    ${records2}=                read table              dbo         NameAgeTable
    log                         ${records2}
    ${metadata}=                get table metadata      dbo         NameAgeTable
    log                         ${metadata}