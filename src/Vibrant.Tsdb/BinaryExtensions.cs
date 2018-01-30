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
         writer.Write( (byte)TypeCode.SByte );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, byte value )
      {
         writer.Write( (byte)TypeCode.Byte );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, short value )
      {
         writer.Write( (byte)TypeCode.Int16 );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, ushort value )
      {
         writer.Write( (byte)TypeCode.UInt16 );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, int value )
      {
         writer.Write( (byte)TypeCode.Int32 );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, uint value )
      {
         writer.Write( (byte)TypeCode.UInt32 );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, long value )
      {
         writer.Write( (byte)TypeCode.Int64 );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, ulong value )
      {
         writer.Write( (byte)TypeCode.UInt64 );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, float value )
      {
         writer.Write( (byte)TypeCode.Single );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, double value )
      {
         writer.Write( (byte)TypeCode.Double );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, decimal value )
      {
         writer.Write( (byte)TypeCode.Decimal );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, bool value )
      {
         writer.Write( (byte)TypeCode.Boolean );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, char value )
      {
         writer.Write( (byte)TypeCode.Char );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, string value )
      {
         writer.Write( (byte)TypeCode.String );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, byte[] value )
      {
         writer.Write( (byte)TypeCode.Object );
         writer.Write( (byte)1 ); // subtype, only 1 supported at the moment
         writer.Write( value.Length );
         writer.Write( value );
      }

      public static void WritePrimitive( this BinaryWriter writer, DateTime value )
      {
         writer.Write( (byte)TypeCode.DateTime );
         writer.Write( value.Ticks );
      }

      public static void WritePrimitive( this BinaryWriter writer, object value )
      {
         var typeCode = Convert.GetTypeCode( value );
         writer.Write( (byte)typeCode );
         switch( typeCode )
         {
            case TypeCode.Boolean:
               writer.Write( (bool)value );
               break;
            case TypeCode.Char:
               writer.Write( (char)value );
               break;
            case TypeCode.SByte:
               writer.Write( (sbyte)value );
               break;
            case TypeCode.Byte:
               writer.Write( (byte)value );
               break;
            case TypeCode.Int16:
               writer.Write( (short)value );
               break;
            case TypeCode.UInt16:
               writer.Write( (ushort)value );
               break;
            case TypeCode.Int32:
               writer.Write( (int)value );
               break;
            case TypeCode.UInt32:
               writer.Write( (uint)value );
               break;
            case TypeCode.Int64:
               writer.Write( (long)value );
               break;
            case TypeCode.UInt64:
               writer.Write( (ulong)value );
               break;
            case TypeCode.Single:
               writer.Write( (float)value );
               break;
            case TypeCode.Double:
               writer.Write( (double)value );
               break;
            case TypeCode.Decimal:
               writer.Write( (decimal)value );
               break;
            case TypeCode.DateTime:
               writer.Write( ( (DateTime)value ).Ticks );
               break;
            case TypeCode.String:
               writer.Write( (string)value );
               break;
            case TypeCode.Object:
               writer.Write( (byte)1 );
               var arr = (byte[])value;
               writer.Write( arr.Length );
               writer.Write( arr );
               break;
            case TypeCode.Empty:
            default:
               throw new ArgumentException( $"The specified type of value ({value.GetType().Name}) is not supported." );
         }
      }

      public static object ReadPrimitive( this BinaryReader reader )
      {
         var typeCode = (TypeCode)reader.ReadByte();
         switch( typeCode )
         {
            case TypeCode.SByte:
               return reader.ReadSByte();
            case TypeCode.Byte:
               return reader.ReadByte();
            case TypeCode.Int16:
               return reader.ReadInt16();
            case TypeCode.UInt16:
               return reader.ReadUInt16();
            case TypeCode.Int32:
               return reader.ReadInt32();
            case TypeCode.UInt32:
               return reader.ReadUInt32();
            case TypeCode.Int64:
               return reader.ReadInt64();
            case TypeCode.UInt64:
               return reader.ReadUInt64();
            case TypeCode.Single:
               return reader.ReadSingle();
            case TypeCode.Double:
               return reader.ReadDouble();
            case TypeCode.Decimal:
               return reader.ReadDecimal();
            case TypeCode.Boolean:
               return reader.ReadBoolean();
            case TypeCode.Char:
               return reader.ReadChar();
            case TypeCode.String:
               return reader.ReadString();
            case TypeCode.DateTime:
               return new DateTime( reader.ReadInt64(), DateTimeKind.Utc );
            case TypeCode.Object:
               var subtype = reader.ReadByte();
               var len = reader.ReadInt32();
               var arr = reader.ReadBytes( len );
               return arr;
            default:
               throw new InvalidOperationException();
         }
      }
   }
}
