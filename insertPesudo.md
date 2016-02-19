
```java
insert(KeyType key, ValueType value, keyComparator comp, vector<PID>path, PID now){
    if (leaf node){
        while (1){
            BWNode * top = table.get(now);
            if (top.type == SPLITNODE){
                insert_split_entry(path[path.size-1]);
            }
            if (top too long){
                if (consolidate() == false){
                    free_blabla();
                    continue;
               }
         }
                while (top too big){
                if (split()==false){
                    free;
                    continue;
                }else {
                    insert_split_entry(parent);
                    top = table.get(parent);
                }
            }
            
            if ()
            
        }
    }else { // internal node
        BWNode * top = table.get(now);
        PID child = top->get_child(key)l
        insert(key, value, path+now, child);
    }
    
}

```
