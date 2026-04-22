[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "data_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared data = let
  Source = AzureStorage.Blobs("https://myadlsg.blob.core.windows.net/"),
  #"Navigation 1" = Source{[Name = "data"]}[Data],
  Navigation = #"Navigation 1"{[#"Folder Path" = "https://myadlsg.blob.core.windows.net/data/", Name = "Drivers.txt"]}[Content],
  #"Imported CSV" = Csv.Document(Navigation, [Delimiter = "#(tab)", Columns = 8, Encoding = 1200, QuoteStyle = QuoteStyle.None]),
  #"Changed column type" = Table.TransformColumnTypes(#"Imported CSV", {{"Column1", Int64.Type}, {"Column2", type text}, {"Column3", type text}, {"Column4", type text}, {"Column5", type text}, {"Column6", type text}, {"Column7", type text}, {"Column8", type text}})
in
  #"Changed column type";
shared data_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null]),
  Navigation_1 = Pattern{[workspaceId = "8d919179-54dc-4df2-8d9f-940230d115c3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "b7fb114b-07b5-4f0a-aeb7-dd6bb681c5a7"]}[Data],
  TableNavigation = Navigation_2{[Id = "data", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
