import cpp
private import semmle.code.cpp.security.FlowSources

module FooSources {
  private class FooExternalSourceFunction extends RemoteFlowSourceFunction {
    FooExternalSourceFunction() { this.hasName("foo") }

    override predicate hasRemoteFlowSource(FunctionOutput output, string description) {
      output.isReturnValue() and
      description = "value returned by " + this.getName()
    }
  }
}
