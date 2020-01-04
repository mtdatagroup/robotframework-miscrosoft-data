*** Settings ***

Resource            resources/common.robot
Library             OperatingSystem
Default Tags        SQL Server      Fixture

Test Setup          Connect to Database     AdventureWorks      ${trusted_connection_string}
Test Teardown       Disconnect from Databases

*** Variables ***
${CSV_FIXTURE}                  ${TESTDATA_DIRECTORY}/test.csv
${XLSX_FIXTURE}                 ${TESTDATA_DIRECTORY}/test.xlsx

*** Test Cases ***
Load CSV data fixture
    ${fixture_contents}=        Get File                ${CSV_FIXTURE}
    Log                         ${fixture_contents}
    truncate table              dbo                     NameAgeTable
    ${row_count}=               table row count         dbo
    ...                                                 NameAgeTable
    should be equal as integers                         ${row_count}        0
    ${row_count}=               load table with CSV     dbo
    ...                                                 NameAgeTable
    ...                                                 ${CSV_FIXTURE}
    should not be equal as integers                     ${row_count}        0
    ${records}=                 read_query              SELECT * FROM dbo.NameAgeTable
    Log                         ${records}
    ${records2}=                read table              dbo         NameAgeTable
    log                         ${records2}
    ${metadata}=                get table metadata      dbo         NameAgeTable
    log                         ${metadata}

Load XLSX data fixture
    truncate table              dbo                     NameAgeTable
    Table Should be Empty       dbo                     NameAgeTable
    load table with xlsx        dbo                     NameAgeTable
    ...                         ${XLSX_FIXTURE}         sample
    ${df}=                      get xlsx                ${XLSX_FIXTURE}
    ...                                                 sample
    log                         ${df}
    table should match xlsx     dbo                     NameAgeTable
    ...                         ${XLSX_FIXTURE}         sample
    query should match xlsx     SELECT * FROM dbo.NameAgeTable WHERE AGE > 40
    ...                         ${XLSX_FIXTURE}         expected1