(* ::Package:: *)

(* ::Title:: *)
(*Elasticsearch utilities*)


BeginPackage["Elasticsearch`"]


(* ::Section:: *)
(*Usage help - symbol declarations*)


(* ::Subsection:: *)
(*Call management*)


ESClient::usage="ESClient[options...] returns a client object with basic access settings. See Options[ESClient] for defaults"


ESCall::usage="ESCall[client, method, path, options...] is a general HTTP call based on the passed client and additional options given on the command line"


ESPost::usage="ESPost[client, path, body] - a simplified POST request. The body is an association that will be converted to the actual JSON payload required by ES"


ESGet::usage="ESGet[client, path] - simplified GET request"


(* ::Subsection:: *)
(*Queries*)


ESQuery::usage="ESQuery[query] define the (top-level) query portion of a search"


ESBoolQuery::usage="ESBoolQuery[boolGroups] combine multiple query types"


ESBoolMust::usage ="ESBoolMust[conditions] - bool query condition must be satisfied"


ESBoolMustNot::usage ="ESBoolMust[conditions] - bool query condition must not be satisfied"


ESBoolShould::usage ="ESBoolMust[conditions] - bool query condition 'should' (or) be satisfied"


ESMatchAllQuery::usage ="ESMatchAllQuery[] - just that"


ESTermQuery::usage="ESTermQuery[field, value] limit to documents where field=value"


ESTermsQuery::usage="ESTermsQuery[field, list] shape a query for documents where 'field' matches the elements in 'list'"


ESRangeQuery::usage="ESRangeQuery[timestampField,from,to] limit query to the range specified"


(* ::Subsection:: *)
(*Aggregations*)


ESAggs::usage="ESAggs[aggs] top level aggregation component"


ESSubAggs::usage="ESSubAggs[agg, subAggs] - adds a sub aggregation to an existing aggregation and returns the full aggregation"


ESCompositeAggregation::usage="ESCompositeAggregation[label, fields, afterKey, chunkSize] set up a paginated aggregation for documents that have the same values for the fields provided. It is sufficient to provide only the two first parameters when setting up a query. Subsequent queries will want 'afterKey' as provided in the response to start at the right spot."


ESGetCompositeAfterKey::usage="ESGetCompositeAfterKey[resp,label] pulls out the key to start the next aggregation on. Returns Null if there is no such key (end of the aggregate)."


ESGetAggregation::usage="ESGetAggregation[resp,label] returns the requested aggregation from a response."


ESCardinalityAggregation::usage="ESCardinalityAggregation[label,field] count (estimate) the number of unique instances of 'field' in the reponse."


ESSumAggregation::usage="ESSumAggregation[label,field] sum the values of 'field'"


ESDateHistogramAggregation::usage="ESDateHistogramAggregation[label, field, interval, timeZone] bin counts into a date histogram"


ESTermsAggregation::usage="ESTermsAggregation[label, field, size] return the top 'size' documents based on the value in 'field'"


(* ::Subsection:: *)
(*Miscellaneous*)


ESSize::usage="ESSize[size] provide return size for query or aggregation"


ToJSON::usage="ToJSON[association] exports the given association to JSON"


ESGetAggregation::usage="ESGetAggregation[resp,label] returns the requested aggregation from a response."


(* ::Section:: *)
(*Call definitions*)


Begin["`Private`"]


(* ::Subsection:: *)
(*Call management*)


Options[ESClient]={
	"Domain"->"localhost",
	"Port"->9200,
	"Scheme"->"http",
	"ContentType"->"application/json",
	"Headers"->{"Accept"->"application/json"},
	"Username"->"", (* Username and Password not used currently *)
	"Password"->"",
	"VerifySecurityCertificates"->True
};


ESClient[opts:OptionsPattern[]]:=<|Join[Options[ESClient],List[opts]]|>/.Association->List


ESCall[client_, method_, path_, opts:OptionsPattern[]]:=Module[{req,combined,auth},
	combined=<|Join[client,List[opts]]|>/.Association->List;
	req=HTTPRequest[path,
		Append[
			<|Method->ToUpperCase[method]|>,
			FilterRules[combined,{"Domain","Port","Scheme","Query","Body","ContentType","Headers"}]
		], 
		FilterRules[combined,Options[HTTPRequest]]/.List->Sequence
	];
	auth:=<|"Username"->("Username"/.client),"Password"->("Password"/.client)|>;
	URLExecute[req,{},"RawJSON"]
]


ESPost[ec_,path_,body_]:=ESCall[ec,"POST",path,"Body"->ExportString[body,"JSON"]]


ESGet[ec_,path_]:=ESCall[ec,"GET",path]


(* ::Subsection:: *)
(*Queries*)


ESQuery[query__Rule]:="query"-><|query|>


ESBoolQuery[boolGroups__Rule]:="bool"-><|boolGroups|>


ESBoolMust[conditions:{__Rule}]:="must"->(<|#|>&/@conditions)


ESBoolMustNot[conditions:{__Rule}]:="must_not"->(<|#|>&/@conditions)


ESBoolShould[conditions:{__Rule}]:="should"->(<|#|>&/@conditions)


ESMatchAllQuery[]:="match_all"-><||>


ESTermQuery[field_,value_]:="term"-><|field->value|>


ESTermsQuery[field_,list_]:="terms"-><|field->list|>


ESRangeQuery[timestampField_,from_,to_]:="range"-><|
	timestampField-><|
		"from"->from, 
		"to"->to, 
		"include_lower"->True,
		"include_upper"->False
	|>
|>


(* ::Subsection:: *)
(*Aggregation*)


ESAggs[aggs__Rule]:="aggs"-><|aggs|> 


ESTermsAggregation[label_,field_,size_Integer]:=label-><|"terms"-><|"field"->field, "size"->size|>|>


ESSubAggs[agg_Rule, subAggs__Rule]:=Module[{label},
	label=agg[[1]];
	label->Append[agg[[2]],subAggs]
]


ESCompositeAggregation[label_,fields_List,afterKey_:Null,chunkSize_:10]:=
	label-><|
		"composite"-><|
			"size"->chunkSize,
			"after"->If[afterKey===Null,<|#->""&/@fields|>,afterKey],
			"sources"->(<|#-><|"terms"-><|"field"->#|>|>|>&/@fields)
		|>
	|>


ESGetCompositeAfterKey[resp_, label_]:=If[
	KeyExistsQ[resp["aggregations"][label],"after_key"],
	resp["aggregations"][label]["after_key"],
	Null
]


ESGetAggregation[resp_, label_]:=resp["aggregations"][label]["buckets"]


ESCardinalityAggregation[label_,field_]:=label-><|
	"cardinality"-><|"field"->field|>
|>


ESSumAggregation[label_,field_]:=label-><|"sum"-><|"field"->field|>|>


ESDateHistogramAggregation[label_,field_,interval_,timeZone_]:=label-><|
	"date_histogram"-><|
		"field"->field,
		"interval"->interval,
		"time_zone"->timeZone
	|>
|>


ESTermsAggregation[label_,field_,size_]:=label-><|
	"terms"-><|"field"->field, "size"->size|>
|>


(* ::Subsection:: *)
(*Miscellaneous*)


ESSize[n_Integer]:="size"->n


ToJSON[assoc_]:=ExportString[assoc,"JSON"]


(* ::Section:: *)
(*Closure*)


End[]


EndPackage[]
