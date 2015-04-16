def map(row)
  keys = []
  values = []
  sentence = row.getString("content")
  for word in sentence.split
    keys << word
    values << '1'
  end
  return keys, values
end

def reduce(key, values)
  output = {}
  total = 0
  for value in values do
    total = total + value.toString.to_i
  end
  output[key] = total.to_s
  return output
end
