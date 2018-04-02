# Identifier generation policies

Those policies aim at giving an identifier to any not-inserted-entity.
Be aware that insertion performance depends on them for persisting **object graphs** :
 - policies that give identifier before setting SQL-insert statement parameters are fully compatible with JDBC batch statement
 - policies that give identifier from a database auto-increment column are not

If your project doesn't expect to persist object graphs (kind of CRUD system) then policy doesn't really matter (and usage of any ORM is no purpose !).
Else you may think about the more efficient policy : the more you insert many values into a table the more you should select an efficient policy.

3 policies are available :
* Identifier given before insertion by code
* Identifier given by the persisting system from an emulated database sequence
* Identifier given by the persisting system from the JDBC generated keys information (a auto-increment column)