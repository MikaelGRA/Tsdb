using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.SqlServer.Server;

namespace Vibrant.Tsdb.Sql
{
   internal static class Sql
   {
      public static readonly SqlMetaData[] InsertParameterMetadata;

      static Sql()
      {
         InsertParameterMetadata = new SqlMetaData[ 3 ];
         InsertParameterMetadata[ 0 ] = new SqlMetaData( "Id", SqlDbType.VarChar, 128 );
         InsertParameterMetadata[ 1 ] = new SqlMetaData( "Timestamp", SqlDbType.DateTime2 );
         InsertParameterMetadata[ 2 ] = new SqlMetaData( "Data", SqlDbType.VarBinary, SqlMetaData.Max );
      }

      private const string Ddl = @"
SET ANSI_NULLS ON

SET QUOTED_IDENTIFIER ON

IF NOT EXISTS (SELECT * FROM [sys].[tables] WHERE [name] = '{0}')
BEGIN
   SET ANSI_PADDING ON

   CREATE TABLE [dbo].[{0}](
	   [Id] [varchar](128) NOT NULL,
	   [Timestamp] [datetime2](7) NOT NULL,
	   [Data] [varbinary](max) NOT NULL,
    CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED 
   (
	   [Id] ASC,
	   [Timestamp] DESC
   )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
   ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

   SET ANSI_PADDING OFF

EXEC('
   CREATE TRIGGER [dbo].[{0}_InsteadOfInsert]
      ON  [dbo].[{0}] 
      INSTEAD OF INSERT
   AS 
   BEGIN
	   -- SET NOCOUNT ON added to prevent extra result sets from
	   -- interfering with SELECT statements.
	   SET NOCOUNT ON;

       -- Insert statements for trigger here
	   MERGE [dbo].[{0}] as [Target]
	   USING INSERTED AS [Row]
	   ON ([Target].[Id] = [Row].[Id] AND [Target].[Timestamp] = [Row].[Timestamp])
	   WHEN MATCHED THEN
	   UPDATE SET [Data] = [Row].[Data]
	   WHEN NOT MATCHED BY TARGET THEN
	   INSERT ([Id], [Timestamp], [Data])
	   VALUES ([Row].[Id], [Row].[Timestamp], [Row].[Data]);
   END
')

EXEC('
   CREATE TYPE [dbo].[Type_{0}_Insert] AS TABLE (
	   [Id] varchar(128) NOT NULL,
	   [Timestamp] datetime2(7) NOT NULL,
	   [Data] varbinary(MAX) NOT NULL
   )
')

EXEC('
   CREATE PROCEDURE [dbo].[{0}_Insert]
	   @Inserts [dbo].[Type_{0}_Insert] READONLY
   AS
   BEGIN
	   -- SET NOCOUNT ON added to prevent extra result sets from
	   -- interfering with SELECT statements.
	   SET NOCOUNT ON;

	   INSERT INTO [dbo].[{0}] ([Id], [Timestamp], [Data])
	   SELECT [Id], [Timestamp], [Data]
	   FROM @inserts;
   END
')

END
";
      public static string GetCreateTableCommand( string tableName )
      {
         return string.Format( Ddl, tableName );
      }

      public static string GetInsertParameterType( string tableName )
      {
         return $"[dbo].[Type_{tableName}_Insert]";
      }

      public static string GetInsertProcedureName( string tableName )
      {
         return $"[dbo].[{tableName}_Insert]";
      }

      public static string GetRangedDeleteCommand( string tableName )
      {
         return $"DELETE FROM [dbo].[{tableName}] WHERE [Id] IN @Ids AND [Timestamp] >= @From AND [Timestamp] < @To";
      }

      public static string GetBottomlessDeleteCommand( string tableName )
      {
         return $"DELETE FROM [dbo].[{tableName}] WHERE [Id] IN @Ids AND [Timestamp] < @To";
      }

      public static string GetDeleteCommand( string tableName )
      {
         return $"DELETE FROM [dbo].[{tableName}] WHERE [Id] IN @Ids";
      }

      public static string GetRangedQuery( string tableName, Sort sort )
      {
         if( sort == Sort.Ascending )
         {
            return $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] IN @Ids AND [Timestamp] >= @From AND [Timestamp] < @To ORDER BY [Id] ASC, [Timestamp] ASC";
         }
         else
         {
            return $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] IN @Ids AND [Timestamp] >= @From AND [Timestamp] < @To ORDER BY [Id] ASC, [Timestamp] DESC";
         }
      }

      public static Query GetSegmentedQuery( string tableName, string id, DateTime? from, DateTime? to, long skip, int take )
      {
         if( from.HasValue && to.HasValue )
         {
            return new Query
            {
               Sql = $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] = @Id AND [Timestamp] >= @From AND [Timestamp] < @To ORDER BY [Timestamp] DESC OFFSET @Skip ROWS FETCH NEXT @Take ROWS ONLY",
               Args = new { Id = id, From = from.Value, To = to.Value, Skip = skip, Take = take }
            };
         }
         else if( !from.HasValue && to.HasValue )
         {
            return new Query
            {
               Sql = $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] = @Id AND [Timestamp] < @To ORDER BY [Timestamp] DESC OFFSET @Skip ROWS FETCH NEXT @Take ROWS ONLY",
               Args = new { Id = id, To = to.Value, Skip = skip, Take = take }
            };
         }
         else if( from.HasValue && !to.HasValue )
         {
            return new Query
            {
               Sql = $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] = @Id AND [Timestamp] >= @From ORDER BY [Timestamp] DESC OFFSET @Skip ROWS FETCH NEXT @Take ROWS ONLY",
               Args = new { Id = id, From = from.Value, Skip = skip, Take = take }
            };
         }
         else
         {
            return new Query
            {
               Sql = $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] = @Id ORDER BY [Timestamp] DESC OFFSET @Skip ROWS FETCH NEXT @Take ROWS ONLY",
               Args = new { Id = id, Skip = skip, Take = take }
            };
         }
      }

      public static string GetBottomlessQuery( string tableName, Sort sort )
      {
         if( sort == Sort.Ascending )
         {
            return $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] IN @Ids AND [Timestamp] < @To ORDER BY [Id] ASC, [Timestamp] ASC";
         }
         else
         {
            return $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] IN @Ids AND [Timestamp] < @To ORDER BY [Id] ASC, [Timestamp] DESC";
         }
      }

      public static string GetQuery( string tableName, Sort sort )
      {
         if( sort == Sort.Ascending )
         {
            return $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] IN @Ids ORDER BY [Id] ASC, [Timestamp] ASC";
         }
         else
         {
            return $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] IN @Ids ORDER BY [Id] ASC, [Timestamp] DESC";
         }
      }

      public static string GetLatestQuery( string tableName )
      {
         return $"SELECT TOP 1 [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] = @Id ORDER BY [Id] ASC, [Timestamp] DESC";
      }
   }
}
