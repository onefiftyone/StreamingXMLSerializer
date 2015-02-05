using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace OneFiftyOne.Serialization.StreamingXMLSerializer
{
    public class StreamingXMLSerializer
    {
        #region PRIVATES
        private static XmlWriterSettings xmlWriterSettings = new XmlWriterSettings()
        {
            Indent = true,
            IndentChars = "\t",
            OmitXmlDeclaration = true
        };
        #endregion

        #region SERIALIZE

        public static void Serialize(object data, string outputFile)
        {
            using (var writer = XmlWriter.Create(outputFile, xmlWriterSettings))
            {
                serializeInternal(data, writer);
            }
        }

        private static void serializeInternal(object data, XmlWriter writer, string nodeName = "")
        {
            if (data is IEnumerable && !(data is string))
            {
                nodeName = string.IsNullOrEmpty(nodeName) ? "List" : nodeName;
                serializeEnumberable(data as IEnumerable, writer, nodeName);
            }
            else
            {
                nodeName = string.IsNullOrEmpty(nodeName) ? data.GetType().Name : nodeName;
                serializeObject(data, writer, nodeName);
            }
        }

        private static void serializeEnumberable(IEnumerable data, XmlWriter writer, string nodeName)
        {
            if (data == null)
                return;

            if (!string.IsNullOrWhiteSpace(nodeName))
                writer.WriteStartElement(nodeName);

            foreach(var datum in data)
                serializeInternal(datum, writer, datum.GetType().Name);

            if (!string.IsNullOrWhiteSpace(nodeName))
                writer.WriteEndElement();
        }

        private static void serializeObject(object data, XmlWriter writer, string nodeName)
        {
            if (data == null)
                return;

            //if primitive type or string, output the value and return
            var dataType = data.GetType();
            if (dataType.IsPrimitive || data is string)
            {
                //primitive must have a node name
                nodeName = string.IsNullOrEmpty(nodeName) ? "Item" : nodeName;
                writer.WriteElementString(nodeName, data.ToString());
                return;
            }

            if (!string.IsNullOrWhiteSpace(nodeName))
                writer.WriteStartElement(nodeName);

            var props = dataType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);

            foreach (var prop in props)
                serializeInternal(prop.GetValue(data, null), writer, prop.Name);
           
            if (!string.IsNullOrWhiteSpace(nodeName))
                writer.WriteEndElement();
        }

        #endregion
    }
}
