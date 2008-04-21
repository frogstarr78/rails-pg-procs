module Inflector
  def triggerize(table_name, events=[], before=false)
    events.join(" or ").gsub(":", "").tr(" ", "_").downcase + "_" + (before ? "before_" : "after_") + table_name.to_s + "_trigger"
  end

  def symbolize(val)
    return "'#{val}'" if val =~ /-/
    ":#{val}"
  end
end
