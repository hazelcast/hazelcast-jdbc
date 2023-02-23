<#-- To render the third-party file.
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)
 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->
<#list licenseMap as e>
<#if e.getValue()?size != 0>
${e.getKey()}
<#list e.getValue() as a>
  ${a.name + ":" + a.version?trim}
</#list>
</#if>
</#list>
