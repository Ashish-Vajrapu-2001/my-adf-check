CREATE PROCEDURE control.sp_GetCDCChanges_CompositeKey
    @SchemaName NVARCHAR(50),
    @TableName NVARCHAR(100),
    @PrimaryKeyColumns NVARCHAR(500),  -- Expects "Col1,Col2"
    @LastSyncVersion BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CurrentVersion BIGINT = CHANGE_TRACKING_CURRENT_VERSION();
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @JoinClause NVARCHAR(MAX) = '';

    -- Construct the JOIN clause for composite keys dynamically
    -- Assumes SQL Server 2016+ (STRING_SPLIT)
    SELECT @JoinClause = @JoinClause + 'AND CT.[' + TRIM(value) + '] = T.[' + TRIM(value) + '] '
    FROM STRING_SPLIT(@PrimaryKeyColumns, ',');

    -- Remove initial 'AND '
    IF LEN(@JoinClause) > 4
        SET @JoinClause = STUFF(@JoinClause, 1, 4, '');

    SET @SQL = N'
        SELECT
            CT.SYS_CHANGE_OPERATION,
            CT.SYS_CHANGE_VERSION,
            T.*,
            @CurrentVersion_IN as _current_sync_version
        FROM CHANGETABLE(CHANGES [' + @SchemaName + '].[' + @TableName + '], @LastSyncVersion_IN) AS CT
        LEFT JOIN [' + @SchemaName + '].[' + @TableName + '] AS T
            ON ' + @JoinClause + '
        WHERE CT.SYS_CHANGE_VERSION <= @CurrentVersion_IN';

    EXEC sp_executesql
        @SQL,
        N'@CurrentVersion_IN BIGINT, @LastSyncVersion_IN BIGINT',
        @CurrentVersion_IN = @CurrentVersion,
        @LastSyncVersion_IN = @LastSyncVersion;
END
