module SqlFormat
  def to_sql_name
    '"' + self.to_s + '"'
  end

  def to_sql_value
    "'#{self}'"
  end
end

class Symbol
  include SqlFormat
end

class String
  include SqlFormat
end
