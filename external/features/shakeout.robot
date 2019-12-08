*** Settings ***

Library             Database        dictionary
Library             OperatingSystem

Test Setup          Connect to Database
Test Teardown       Disconnect from Database

*** Variables ***
${SERVER}               10.0.0.126
${CONNECTION_STRING}    user:pass@${SERVER}/AdventureWorksDW2017
${FIXTURE}              /usr/src/external/fixtures/test.csv

*** Test Cases ***
Play with SQL Server
    set return type     pandas
    ${df}=              read query          SELECT TOP 10 * FROM dbo.DimCustomer
    Log                 ${df}
    set return type     dictionary
    ${dict}=            read query          SELECT TOP 10 * FROM dbo.DimCustomer
    Log                 ${dict}
    ${rec_count}=       read scalar         SELECT COUNT(*) FROM dbo.DimCustomer
    Log                 ${rec_count}
    ${non_scalar}=      read scalar         SELECT * FROM dbo.DimCustomer
    Log                 ${non_scalar}
    ${tables}=          list tables         dbo
    log                 ${tables}
    ${exists}=          table exists        dbo     DimCustomer
    should be true      ${exists}

Load data fixtures
    ${fixture_contents}=    Get File                ${FIXTURE}
    Log                     ${fixture_contents}
    truncate table          dbo                     NameAgeTable
    ${row_count}=           row count               dbo                 NameAgeTable
    should be equal as integers                     ${row_count}        0
    ${row_count}=           load table with CSV     ${FIXTURE}          dbo         NameAgeTable
    should not be equal as integers                 ${row_count}        0
    ${records}=             read_query              SELECT * FROM dbo.NameAgeTable
    Log                     ${records}
    ${records2}=            read table              dbo         NameAgeTable
    log                     ${records2}
    ${metadata}=            get table metadata      dbo         NameAgeTable
    log                     ${metadata}

Play with Procedures and Functions
    ${functions}=           list functions
    log                     ${functions}
    ${procs}=               list procedures
    log                     ${procs}
    ${result}=              read query              select dbo.udfTwoDigitZeroFill(1)
    log                     ${result}
    ${result2}=             execute procedure        SelectAllCustomers
    log                     ${result2}
    ${params}=              create list              1
    ${result3}=             execute procedure        SelectAllCustomersWithTotalChildren    ${params}
    log                     ${result3}

*** Keywords ***
Connect to Database
    Connect To MsSql    AdventureWorks      ${CONNECTION_STRING}

Disconnect from Database
    Disconnect