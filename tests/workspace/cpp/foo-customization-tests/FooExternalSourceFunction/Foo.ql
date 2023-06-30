import cpp
import foo.cpp_customizations.Customizations
import semmle.code.cpp.security.FlowSources

select any(RemoteFlowSource s)