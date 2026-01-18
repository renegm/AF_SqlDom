using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Azure.Functions.Worker.Http;
using System.Net;
using System.Collections;
using System.Reflection;
using System.Text.Json;
using System.Collections.Concurrent;


namespace AF_SqlDom;

public class TreeNode
{
    public string TreePath { get; set; } = string.Empty;
    public string NodeName { get; set; } = string.Empty;
    public string Type { get; set; } = string.Empty;
    public int FirstTokenIndex { get; set; }
    public int LastTokenIndex { get; set; }
    public int StartOffset { get; set; }
    public int StartLine { get; set; }
    public int StartColumn { get; set; }
    public int FragmentLength { get; set; }
    public string? Identifier { get; set; }
    public Dictionary<string, string>? OtherValues { get; set; }
}

public class SqlDomRenamePolicy : JsonNamingPolicy
{
    public static readonly Dictionary<string, string> Map = new()
    {
        { "TreePath", "a" },
        { "NodeName", "b" },
        { "Type", "c" },
        { "FirstTokenIndex", "d" },
        { "LastTokenIndex", "e" },
        { "StartOffset", "f" },
        { "StartLine", "g" },
        { "StartColumn", "h" },
        { "FragmentLength", "i" },
        { "Identifier", "j" },
        { "OtherValues", "k" },
        { "TokenType", "l" },
        { "Offset", "m" },
        { "Line", "n" },
        { "Column", "o" },
        { "Text", "p" },
        { "Message", "q" },
        { "Number", "r" }
    };

    public override string ConvertName(string name)
        => Map.TryGetValue(name, out var shortName) ? shortName : name;
}


public class SQLDomAST(ILogger<SQLDomAST> logger)
{
    private readonly ILogger<SQLDomAST> _logger = logger;

    private static readonly HashSet<string> SkippedPropName = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "ScriptTokenStream",
            "FirstTokenIndex",
            "LastTokenIndex",
            "StartLine",
            "StartColumn",
            "StartOffset",
            "FragmentLength",
            "SchemaIdentifier",
            "BaseIdentifier",
            "ServerIdentifier",
            "DatabaseIdentifier"
        };

    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> _propCache = new ConcurrentDictionary<Type, PropertyInfo[]>();
    private static readonly ConcurrentDictionary<Type, string> _typeNameCache = new ConcurrentDictionary<Type, string>();

    [Function("SQLDomAST")]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData req, FunctionContext context)
    {
        _logger.LogInformation("C# HTTP trigger function processed a request.");

        using var reader = new StreamReader(req.Body);
        string? sqlscript = await reader.ReadToEndAsync();

        if (string.IsNullOrWhiteSpace(sqlscript))
        {
            var emptyResponse = req.CreateResponse(HttpStatusCode.OK);
            emptyResponse.Headers.Add("Content-Type", "application/json");
            await emptyResponse.WriteStringAsync("{}");
            return emptyResponse;
        }
        else if (sqlscript == "META")
        {
            var meta = new
                {
                    Keys = SqlDomRenamePolicy.Map,
                    TokenTypes = Enum.GetValues<TSqlTokenType>()
                        .Select(t => new { n = t.ToString(), v = (int)t })
                        .ToList()
                };

                var metaResponse = req.CreateResponse(HttpStatusCode.OK);
                metaResponse.Headers.Add("Content-Type", "application/json");

                await metaResponse.WriteStringAsync(JsonSerializer.Serialize(meta));
        
            return metaResponse;
        }

        IList<TreeNode> TreeResult = new List<TreeNode>();

        var response = req.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/json");
        // response.Headers.Add("Columns-Dictionary", "a=TreePath,b=NodeName,c=Type,d=FirstTokenIndex,e=LastTokenIndex,f=StartOffset,g=StartLine,h=StartColumn,i=FragmentLength,j=Identifier,k=OtherValues,l=TokenType,m=Offset,n=Line,o=Column,p=Text,q=Message,r=Number");
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = new SqlDomRenamePolicy(),
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
        };

        using var rdr = new StringReader(sqlscript);
        {
            IList<ParseError> errors = new List<ParseError>();
            var parser = new TSql170Parser(true, SqlEngineType.All);
            var tree = parser.Parse(rdr, out errors);

            if (errors.Count > 0)
            {
                var FinalResult = new { Errors = errors };
                await response.WriteStringAsync(JsonSerializer.Serialize(FinalResult, options));
            }
            else
            {
                FillTree("/", "Root", tree, TreeResult);
                var FinalResult = new { Tree = TreeResult, TokenStream = tree.ScriptTokenStream };
                await response.WriteStringAsync(JsonSerializer.Serialize(FinalResult, options));
            }
        }

        return response;
    }

    private static void FillTree(string TreePath, string NodeName, TSqlFragment Node, IList<TreeNode> Tree)
    {
        var nodeType = Node.GetType();
        var props = _propCache.GetOrAdd(nodeType, t => t.GetProperties());
        var tokenStream = Node.ScriptTokenStream;
        int first = Node.FirstTokenIndex;
        int last = Node.LastTokenIndex;
        var typeName = _typeNameCache.GetOrAdd(nodeType, t => t.Name);

        var Current = new TreeNode
        {
            TreePath = TreePath,
            NodeName = NodeName,
            Type = typeName,
            FirstTokenIndex = first,
            LastTokenIndex = last,
            StartOffset = Node.StartOffset,
            StartLine = Node.StartLine,
            StartColumn = Node.StartColumn,
            FragmentLength = Node.FragmentLength
        };

        Dictionary<string, string>? OtherValues = null;
        int i = -1;

        if (typeName == "Identifier")
        {
            Current.Identifier = tokenStream[first].Text;
        }
        else
        {
            foreach (var prop in props)
            {
                if (prop.GetIndexParameters().Length > 0)
                    continue;

                string PropName = prop.Name;
                if (SkippedPropName.Contains(PropName))
                    continue;

                object? PropValue = prop.GetValue(Node);
                if (PropValue == null)
                    continue;

                var type = PropValue.GetType();

                if (type.IsPrimitive || type.IsValueType || type == typeof(string))
                {
                    OtherValues ??= [];
                    OtherValues[PropName] = PropValue.ToString()!;
                }
                else if (PropName == "Identifiers")
                {
                    var sb = new StringBuilder();
                    // for (int j = first; j <= last; j++)
                    //     sb.Append(tokenStream[j].Text);
                    // Tempting but doesn't work because Tokens can contain comments like in 
                    // SELECT C/*qqq*/.EndTime  FROM dbo.CommandLog AS C; 
                    foreach (TSqlFragment Child in (IEnumerable)PropValue)
                    {
                        sb.Append(Child.ScriptTokenStream[Child.FirstTokenIndex].Text).Append('.');
                    }
                    sb.Length--;
                    Current.Identifier = sb.ToString();
                }
                else if (typeof(IEnumerable).IsAssignableFrom(type) && type != typeof(string))
                {
                    foreach (var Child in (IEnumerable)PropValue)
                    {
                        i++;
                        FillTree(TreePath + i + "/", PropName, (TSqlFragment)Child, Tree);
                    }
                }
                else
                {
                    i++;
                    FillTree(TreePath + i + "/", PropName, (TSqlFragment)PropValue, Tree);
                }
            }
        }

        if (OtherValues is not null)
            Current.OtherValues = OtherValues;

        Tree.Add(Current);
    }

}