using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public static class BinaryExtensions
   {
      public static void WritePrimitive( this BinaryWriter writer, sbyte value )
      {
         writer.Write( BinaryConstants.Code_Sbyte );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, byte value )
      {
         writer.Write( BinaryConstants.Code_Byte );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, short value )
      {
         writer.Write( BinaryConstants.Code_Short );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, ushort value )
      {
         writer.Write( BinaryConstants.Code_Ushort );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, int value )
      {
         writer.Write( BinaryConstants.Code_Int );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, uint value )
      {
         writer.Write( BinaryConstants.Code_Uint );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, long value )
      {
         writer.Write( BinaryConstants.Code_Long );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, ulong value )
      {
         writer.Write( BinaryConstants.Code_Ulong );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, float value )
      {
         writer.Write( BinaryConstants.Code_Float );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, double value )
      {
         writer.Write( BinaryConstants.Code_Double );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, decimal value )
      {
         writer.Write( BinaryConstants.Code_Decimal );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, bool value )
      {
         writer.Write( BinaryConstants.Code_Boolean );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, char value )
      {
         writer.Write( BinaryConstants.Code_Char );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, string value )
      {
         writer.Write( BinaryConstants.Code_String );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, DateTime value )
      {
         writer.Write( BinaryConstants.Code_DateTime );
         writer.Write( value.Ticks );
      }

      public static void WritePrimitive( this BinaryWriter writer, TimeSpan value )
      {
         writer.Write( BinaryConstants.Code_TimeSpan );
         writer.Write( value.Ticks );
      }

      public static object ReadPrimitive( this BinaryReader reader )
      {
         var code = reader.ReadByte();
         switch( code )
         {
            case BinaryConstants.Code_Sbyte:
               return reader.ReadSByte();
            case BinaryConstants.Code_Byte:
               return reader.ReadByte();
            case BinaryConstants.Code_Short:
               return reader.ReadInt16();
            case BinaryConstants.Code_Ushort:
               return reader.ReadUInt16();
            case BinaryConstants.Code_Int:
               return reader.ReadInt32();
            case BinaryConstants.Code_Uint:
               return reader.ReadUInt32();
            case BinaryConstants.Code_Long:
               return reader.ReadInt64();
            case BinaryConstants.Code_Ulong:
               return reader.ReadUInt64();
            case BinaryConstants.Code_Float:
               return reader.ReadSingle();
            case BinaryConstants.Code_Double:
               return reader.ReadDouble();
            case BinaryConstants.Code_Decimal:
               return reader.ReadDecimal();
            case BinaryConstants.Code_Boolean:
               return reader.ReadBoolean();
            case BinaryConstants.Code_Char:
               return reader.ReadChar();
            case BinaryConstants.Code_String:
               return reader.ReadString();
            case BinaryConstants.Code_DateTime:
               return new DateTime( reader.ReadInt64(), DateTimeKind.Utc );
            case BinaryConstants.Code_TimeSpan:
               return new TimeSpan( reader.ReadInt64() );
            default:
               throw new InvalidOperationException();
         }
      }
   }
}
