using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
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
                writer.WriteStartElement(nodeName);
                writer.WriteValue(data);
                writer.WriteEndElement();
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

        #region DESERIALIZE

        public static T Deserialize<T>(string inputFile)
        {
            using (var reader = XmlReader.Create(inputFile))
            {
                var data = deserializeInternal(reader, typeof(T), true);
                return (T)data;
            }
        }

        public static IEnumerable<T> DeserializeEnumerable<T>(string inputFile)
        {
            using (var reader = XmlReader.Create(inputFile))
            {
                var objType = typeof(T);
                reader.ReadToFollowing("List");
                reader.MoveToContent();
                using (var subReader = reader.ReadSubtree())
                {
                    foreach (var obj in deserializeEnumberable(subReader, objType))
                        yield return (T)obj;
                }
            }
        }

        private static object deserializeInternal(XmlReader reader, Type t, bool isRoot = false)
        {
            var data = createSafeInstance(t); 
            if (data is IList)
            {
                var objType = (t.IsGenericType) ? t.GetGenericArguments().First() : t;
                if (isRoot) reader.ReadToFollowing("List"); else reader.Read();
                reader.MoveToContent();
                using (var subReader = reader.ReadSubtree())
                {
                    var dataObj = deserializeEnumberable(subReader, objType).ToList();

                    var genericType = t.GetGenericArguments().FirstOrDefault();
                    if (genericType == null)
                        throw new Exception("Non-Generic IList's are not supported");

                    var methodSig = typeof(StreamingXMLSerializer).GetMethod("constructIList", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
                    var genMethod = methodSig.MakeGenericMethod(new Type[] { genericType });
                    data = genMethod.Invoke(null, new object[] { dataObj });
                }
            }
            else
            {
                var objType = (t.IsGenericType) ? t.GetGenericArguments().First() : t;
                reader.MoveToContent();
                using (var subReader = reader.ReadSubtree())
                {
                    data = deserializeObject(subReader, objType);
                }
            }

            return data;
        }

        private static object deserializeObject(XmlReader reader, Type objType)
        {
            reader.MoveToContent();
            var rootNode = reader.LocalName; 

            if (objType.IsPrimitive || objType.Name.Equals("String", StringComparison.CurrentCultureIgnoreCase))
            {
                return reader.ReadContentAs(objType, null);
            }

            var data = Activator.CreateInstance(objType);
           
            while (reader.Read())
            {
                reader.MoveToContent();
                if (reader.LocalName == rootNode)
                    break;

                var propName = reader.LocalName;
                var objProp = objType.GetProperty(propName, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
                if (objProp == null)
                    throw new InvalidCastException(string.Format("Type '{0}' does not match the XML: {1}", objType.Name, reader.ReadInnerXml()));

                if (objProp.PropertyType.IsPrimitive || objProp.PropertyType == typeof(string))
                {
                    try
                    {
                        objProp.SetValue(data, reader.ReadElementContentAs(objProp.PropertyType, null), null);
                    }
                    catch (Exception e)
                    {
                        throw new Exception(string.Format("Unable to set '{0}' property '{1}' from the XML: '{2}'", objType.Name, objProp.Name, reader.ReadString()), e);
                    }
                }
                else
                {
                    using (var subReader = reader.ReadSubtree())
                    {
                        var propVal = deserializeInternal(subReader, objProp.PropertyType);
                        try
                        {
                            objProp.SetValue(data, propVal, null);
                        }
                        catch (Exception e)
                        {
                            throw new Exception(string.Format("Unable to set '{0}' property '{1}' from the XML: '{2}'", objType.Name, objProp.Name, reader.ReadString()), e);
                        }
                    }
                }
            }

            return data;
        }

        private static IEnumerable<object> deserializeEnumberable(XmlReader reader, Type objType)
        {
            reader.MoveToContent();
            while (reader.Read())
            {
                reader.MoveToContent();
                if (reader.NodeType == XmlNodeType.Element)
                {
                    using (var subReader = reader.ReadSubtree())
                    {
                        yield return deserializeInternal(subReader, objType);
                    }
                }
            }
        }

        #endregion

        #region HELPERS

        private static object createSafeInstance(Type t)
        {
            if (t.IsInterface)
            {
                var genericType = t.GetGenericArguments().FirstOrDefault();
                if (genericType == null)
                    throw new Exception("Non-Generic IEnumerable's are not supported");

                var genListType = typeof(List<>).MakeGenericType(new Type[] { genericType });
                if (!t.IsAssignableFrom(genListType))
                    throw new Exception("Cannot create an instance of type: "+ t.FullName);

                return Activator.CreateInstance(genListType);
            }
            else
                return Activator.CreateInstance(t);
        }


        private static IList<T> constructIList<T>(IEnumerable data)
        {
            var dataList = new List<T>();
            foreach (var datum in data)
                dataList.Add((T)datum);

            return dataList;
        }

        #endregion
    }
}
