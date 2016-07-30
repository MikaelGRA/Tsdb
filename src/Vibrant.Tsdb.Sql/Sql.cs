using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Sql
{
    internal static class Sql
    {
      private const string Ddl = @"
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

IF NOT EXISTS (SELECT * FROM [sys].[tables] WHERE [name] = '{0}')
BEGIN
   SET ANSI_PADDING ON
   GO

   CREATE TABLE [dbo].[{0}](
	   [Id] [varchar](256) NOT NULL,
	   [Timestamp] [datetime2](7) NOT NULL,
	   [Data] [varbinary](max) NOT NULL,
    CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED 
   (
	   [Id] ASC,
	   [Timestamp] DESC
   )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
   ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

   GO

   SET ANSI_PADDING OFF
   GO

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
   GO
END
GO
";
      public static string GetCreateTableCommand( string tableName )
      {
         return string.Format( Ddl, tableName );
      }

      public static string GetInsertCommand( string tableName )
      {
         return $"INSERT INTO [dbo].[{tableName}] ([Id], [Timestamp], [Data]) VALUES (@Id, @Timestamp, @Data)";
      }

      public static string GetRangedDeleteCommand( string tableName )
      {
         return $"DELETE FROM [dbo].[{tableName}] WHERE [Id] IN @Ids AND [Timestamp] >= @From AND [Timestamp] < @To";
      }

      public static string GetDeleteCommand( string tableName )
      {
         return $"DELETE FROM [dbo].[{tableName}] WHERE [Id] IN @Ids";
      }

      public static string GetRangedQuery( string tableName )
      {
         return $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] IN @Ids AND [Timestamp] >= @From AND [Timestamp] < @To ORDER BY [Id] ASC, [Timestamp] DESC";
      }

      public static string GetQuery( string tableName )
      {
         return $"SELECT [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] IN @Ids ORDER BY [Id] ASC, [Timestamp] DESC";
      }

      public static string GetLatestQuery( string tableName )
      {
         return $"SELECT TOP 1 [Id], [Timestamp], [Data] FROM [dbo].[{tableName}] WHERE [Id] = @Id ORDER BY [Id] ASC, [Timestamp] DESC";
      }
   }
}
