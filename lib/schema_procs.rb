module SchemaProcs
  @@_cascade_or_restrict = Proc.new {|which| which     ? 'CASCADE' : 'RESTRICT' }
  @@_strict_or_null      = Proc.new {|strict| strict   ? 'STRICT' : 'CALLED ON NULL INPUT' }
  @@_definer_or_invoker  = Proc.new {|definer| definer ? 'DEFINER' : 'INVOKER' }
  @@_behavior            = Proc.new {|volatile| %w{immutable stable volatile}.grep(/^#{volatile[0,1]}.+/).to_s }

  def cascade_or_restrict(cascade=false)
    @@_cascade_or_restrict.call(cascade)
  end
  def strict_or_null(is_strict=false)
    @@_strict_or_null.call(is_strict)
  end
  def definer_or_invoker(definer=false)
    @@_definer_or_invoker.call(definer)
  end
  def behavior(volatile='v')
    @@_behavior.call(volatile)
  end
end
