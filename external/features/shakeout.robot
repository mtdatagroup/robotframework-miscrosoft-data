*** Settings ***

Library             Database
Library             OperatingSystem

*** Variables ***
${COMPANY_NAME}         Still working on it
${SERVER}               10.0.0.126
${CONNECTION_STRING}    user:pass@${SERVER}/AdventureWorksDW2017
${FIXTURE}              /usr/src/external/fixtures/test.csv

*** Test Cases ***
Hello from us
    log to console  hello from ${COMPANY_NAME}

Play with SQL Server
    Connect To MsSql    AdventureWorks      ${CONNECTION_STRING}
    ${df}=              read query          SELECT * FROM dbo.DimCustomer
    Log                 ${df}
    ${rec_count}=       read scalar         SELECT COUNT(*) FROM dbo.DimCustomer
    Log                 ${rec_count}
    ${non_scalar}=      read scalar         SELECT * FROM dbo.DimCustomer
    Log                 ${non_scalar}
    ${tables}=          list tables         dbo
    log                 ${tables}
    ${exists}=          table exists        dbo     DimCustomer
    should be true      ${exists}
    disconnect

Load data fixtures
    ${fixture_contents}=    Get File                ${FIXTURE}
    Log                     ${fixture_contents}
    Connect To MsSql        AdventureWorks          ${CONNECTION_STRING}
    ${row_count}=           load table with CSV     ${FIXTURE}          dbo         NameAgeTable
    should not be equal as integers                 ${row_count}        0

*** Keywords ***
