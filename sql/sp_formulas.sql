DROP TABLE Elementos cascade constraint;
DROP TABLE Moleculas cascade constraint;
DROP TABLE Composicion cascade constraint;
DROP SEQUENCE seq_molId;

CREATE TABLE Elementos (
  simbolo varchar(3),
  nombre varchar(20) UNIQUE NOT NULL,
  pesoAtomico integer NOT NULL,
  PRIMARY KEY(simbolo)
);

CREATE TABLE Moleculas (
  id integer,
  nombre varchar(20) UNIQUE NOT NULL,
  pesoMolecular integer NOT NULL,
  formula varchar(20) UNIQUE NOT NULL,
  PRIMARY KEY(id)
);

CREATE TABLE Composicion (
  simbolo varchar(3),
  idMolecula integer,
  nroAtomos integer NOT NULL,  
  PRIMARY KEY(simbolo, idMolecula),
  FOREIGN KEY(simbolo) REFERENCES Elementos(simbolo),
  FOREIGN KEY(IdMolecula) REFERENCES Moleculas(id)
);

CREATE SEQUENCE seq_molId; 

insert into Elementos(simbolo, nombre, pesoAtomico) values ('H','Hidrogeno', 1);
insert into Elementos(simbolo, nombre, pesoAtomico) values('O','Oxigeno', 18);

insert into Moleculas(id, nombre, pesoMolecular, formula) values(seq_molId.nextval, 'Agua', 20, 'H2O');

insert into Composicion(simbolo, idMolecula, nroAtomos) values('H', seq_molId.currval, 2);
insert into Composicion(simbolo, idMolecula, nroAtomos) values('O', seq_molId.currval, 1);

commit;

--Creamos los procedimientos.
CREATE OR REPLACE TYPE NESTED_TYPE IS TABLE OF VARCHAR2(20);
/
------------------------------------------------------------------------------------------------------------------------------
--- BORRAR POR ID
------------------------------------------------------------------------------------------------------------------------------
create or replace procedure borrarMoleculaId(id_mol INTEGER) is
  
  MOLECULA_NO_EXISTE  exception;
  PRAGMA EXCEPTION_INIT(MOLECULA_NO_EXISTE, -20000);

begin
  
  --Comprobamos cuantas filas borra
  delete from Composicion
  where idMolecula=id_mol;
  
  --Si no se ha borao ninguna fila es que el id no existe.
  if(sql%ROWCOUNT)=0 then
    raise MOLECULA_NO_EXISTE;
  else
    delete from Moleculas
    where id=id_mol;
  end if;
  
  commit;
exception
  when MOLECULA_NO_EXISTE then 
    rollback; 
    dbms_output.put_line('-> No existe molecula con ese id.');

end;
/

------------------------------------------------------------------------------------------------------------------------------
--- BORRAR POR NOMBRE
------------------------------------------------------------------------------------------------------------------------------
create or replace procedure borrarMoleculaNombre(nombre_mol varchar) is
  
  v_id_mol Integer;
  
  NOMBRE_MOLECULA_NO_EXISTE  exception;
  PRAGMA EXCEPTION_INIT(NOMBRE_MOLECULA_NO_EXISTE, -20000);

begin
  
  --Comprobamos si existe el nombre, sino lanza excepcion.
  select id into v_id_mol
  from Moleculas
  where nombre=nombre_mol;
  
  --Existe nombre, con el id borramos en composicion y Moleculas
  delete from Composicion
  where idMolecula=v_id_mol;
  
  delete from Moleculas
  where id=v_id_mol;
  
  commit;
exception
  when no_data_found then
    rollback;
    dbms_output.put_line('->Nombre de la molecula no existe.');

end;
/

------------------------------------------------------------------------------------------------------------------------------
--- ACTUALIZAR POR ID
------------------------------------------------------------------------------------------------------------------------------
create or replace procedure actualizarMoleculaId(p_id Integer, p_simbolo varchar, p_nro Integer) is
  v_simbolo varchar(20);
  v_id_mol Integer;
  v_nroAtomos Integer;
  --Tipo de la fila del cursor.
  v_filaCursor Composicion%ROWTYPE;
  
  v_pesoMolecularTotal Integer:=0;
  v_formulaFinal varchar(20):='';
  
  cursor c_nroSimbolo is 
    SELECT * FROM Composicion WHERE idMolecula=p_id;
  
  cursor c_joinElemComp is
    SELECT elementos.simbolo, elementos.nombre, elementos.pesoatomico, composicion.nroatomos FROM Elementos inner join Composicion ON Elementos.simbolo = composicion.simbolo and composicion.idMolecula=p_id;
  
  v_j_simbolo Elementos.simbolo%TYPE;
  v_j_nombre Elementos.nombre%TYPE;
  v_j_pesoAtomico Elementos.pesoatomico%TYPE;
  v_j_nroAtomos Composicion.nroatomos%TYPE;
  
  FORMULA_YA_EXISTENTE  exception;
  ID_INEXISTENTE exception;
  MOLECULA_NO_CONTIENE_SIMBOLO  exception;
  
  PRAGMA EXCEPTION_INIT(FORMULA_YA_EXISTENTE, -20000);
  PRAGMA EXCEPTION_INIT(MOLECULA_NO_CONTIENE_SIMBOLO, -20001);
  PRAGMA EXCEPTION_INIT(ID_INEXISTENTE, -20002);

begin
    
  --Comprobamos que existe una molecula con ese id.
  SELECT count(idmolecula) into v_id_mol FROM Composicion WHERE idMolecula=p_id;
  if(v_id_mol <= 0) then
    raise ID_INEXISTENTE;
  else 
    dbms_output.put_line('Molecula contiene id.');
  end if;

  --Comprobamos que el nroAtomico y el simbolo no son los mismos que los existentes con un cursor.
  open c_nroSimbolo; 
  loop
    fetch c_nroSimbolo into v_filaCursor;
    exit when c_nroSimbolo%notfound;
    --Comprobamos mediante el simbolo si tienen el mismo nroAtomico
    if (v_filaCursor.simbolo = p_simbolo and v_filaCursor.nroAtomos = p_nro) then
      RAISE FORMULA_YA_EXISTENTE;
    end if;
  end loop;
  close c_nroSimbolo;
  
  
  --Una vez que sabemos que la formula no existia, actualizamos en Composicion.
  UPDATE Composicion set nroAtomos=p_nro WHERE idMolecula=p_id and simbolo=p_simbolo;
  --Comprobamos si el simbolo existia o no existia
  if(sql%ROWCOUNT)=0 then
    raise MOLECULA_NO_CONTIENE_SIMBOLO;
  else
    dbms_output.put_line('Se ha actualizado.');
  end if;
  
  
  --Tambien se puede manejar el cursor con un bucle for.
  for v_elem in c_joinElemComp loop
    v_pesoMolecularTotal := v_pesomoleculartotal + (v_elem.pesoAtomico*v_elem.nroAtomos);
    v_formulaFinal := v_formulaFinal || v_elem.simbolo;
    if (v_elem.nroAtomos>1) then
      v_formulaFinal := v_formulaFinal || v_elem.nroAtomos;
    end if;
  end loop;

  --Realizamos el update en la tabla Moleculas
  UPDATE Moleculas set pesoMolecular=v_pesoMolecularTotal, formula=v_formulafinal WHERE id=p_id;
  --Comprobamos si se ha actualizado
  if(sql%ROWCOUNT)=0 then
    raise ID_INEXISTENTE;
  else
    dbms_output.put_line('Se ha actualizado.');
  end if;
  
  commit;

exception
  when no_data_found then
    rollback;
    dbms_output.put_line('->Not found.');
  when ID_INEXISTENTE then
    rollback;
    dbms_output.put_line('->No existe molecula con ese id.');
  when MOLECULA_NO_CONTIENE_SIMBOLO then
    rollback;
    dbms_output.put_line('->Molecula no contiene simbolo.'); 
  when FORMULA_YA_EXISTENTE then
    rollback;
    dbms_output.put_line('->Molecula con formula existente.');

end;
/
------------------------------------------------------------------------------------------------------------------------------
--- ACTUALIZAR POR NOMBRE
------------------------------------------------------------------------------------------------------------------------------
create or replace procedure actualizarMoleculaNombre(p_nombre varchar, p_simbolo varchar, p_nro Integer) is
  v_simbolo varchar(20);
  v_id_mol Integer;
  v_nombre_mol varchar(20);
  v_nroAtomos Integer;
  --Tipo de la fila del cursor.
  v_filaCursor Composicion%ROWTYPE;
  
  v_pesoMolecularTotal Integer:=0;
  v_formulaFinal varchar(20):='';
  
  cursor c_nroSimbolo is 
    SELECT * FROM Composicion WHERE idMolecula=v_id_mol;
  
  cursor c_joinElemComp is
    SELECT elementos.simbolo, elementos.nombre, elementos.pesoatomico, composicion.nroatomos FROM Elementos inner join Composicion ON Elementos.simbolo = composicion.simbolo and composicion.idMolecula=v_id_mol;
  
  v_j_simbolo Elementos.simbolo%TYPE;
  v_j_nombre Elementos.nombre%TYPE;
  v_j_pesoAtomico Elementos.pesoatomico%TYPE;
  v_j_nroAtomos Composicion.nroatomos%TYPE;
  
  FORMULA_YA_EXISTENTE  exception;
  NOMBRE_INEXISTENTE exception;
  MOLECULA_NO_CONTIENE_SIMBOLO  exception;
  
  PRAGMA EXCEPTION_INIT(FORMULA_YA_EXISTENTE, -20000);
  PRAGMA EXCEPTION_INIT(MOLECULA_NO_CONTIENE_SIMBOLO, -20001);
  PRAGMA EXCEPTION_INIT(NOMBRE_INEXISTENTE, -20002);

begin
    
  --sacamos el id asociado a ese nombre
  
  --Comprobamos que existe una molecula con ese nombre.
  SELECT count(nombre) into v_nombre_mol FROM Moleculas WHERE nombre=p_nombre;
  if(v_nombre_mol <= 0) then
    raise NOMBRE_INEXISTENTE;
  else 
    dbms_output.put_line('Molecula contiene nombre.');
  end if;

  --sacamos el id asociado a ese nombre para repetir proceso
  select id into v_id_mol FROM Moleculas WHERE nombre=p_nombre;

  --Comprobamos que el nroAtomico y el simbolo no son los mismos que los existentes con un cursor.
  open c_nroSimbolo; 
  loop
    fetch c_nroSimbolo into v_filaCursor;
    exit when c_nroSimbolo%notfound;
    --Comprobamos mediante el simbolo si tienen el mismo nroAtomico
    if (v_filaCursor.simbolo = p_simbolo and v_filaCursor.nroAtomos = p_nro) then
      RAISE FORMULA_YA_EXISTENTE;
    end if;
  end loop;
  close c_nroSimbolo;
  
  
  --Una vez que sabemos que la formula no existia, actualizamos en Composicion.
  UPDATE Composicion set nroAtomos=p_nro WHERE idMolecula=v_id_mol and simbolo=p_simbolo;
  --Comprobamos si el simbolo existia o no existia
  if(sql%ROWCOUNT)=0 then
    raise MOLECULA_NO_CONTIENE_SIMBOLO;
  else
    dbms_output.put_line('Se ha actualizado.');
  end if;
  
  
  --Tambien se puede manejar el cursor con un bucle for.
  --Calculamos el pesoMolecular y concatenamos la formula.
  for v_elem in c_joinElemComp loop
    v_pesoMolecularTotal := v_pesomoleculartotal + (v_elem.pesoAtomico*v_elem.nroAtomos);
    v_formulaFinal := v_formulaFinal || v_elem.simbolo;
    if (v_elem.nroAtomos>1) then
      v_formulaFinal := v_formulaFinal || v_elem.nroAtomos;
    end if;
  end loop;

  --Realizamos el update en la tabla Moleculas
  UPDATE Moleculas set pesoMolecular=v_pesoMolecularTotal, formula=v_formulafinal WHERE id=v_id_mol;
  --Comprobamos si se ha actualizado
  if(sql%ROWCOUNT)=0 then
    raise NOMBRE_INEXISTENTE;
  end if;
  
  commit;

exception
  when no_data_found then
    rollback;
    dbms_output.put_line('->Not found.');
  when NOMBRE_INEXISTENTE then
    rollback;
    dbms_output.put_line('->No existe molecula con ese id.');
  when MOLECULA_NO_CONTIENE_SIMBOLO then
    rollback;
    dbms_output.put_line('->Molecula no contiene simbolo.'); 
  when FORMULA_YA_EXISTENTE then
    rollback;
    dbms_output.put_line('->Molecula con formula existente.');

end;
/

------------------------------------------------------------------------------------------------------------------------------
--- INSERTAR
------------------------------------------------------------------------------------------------------------------------------


create or replace procedure insertarMolecula(p_nombre varchar, p_simbolos NESTED_TYPE, p_nros NESTED_TYPE) is
  
  v_nombre_mol varchar(20);
  v_formulaFinal varchar(20):='';
  v_pesoMolecularTotal Integer:=0;
  v_pesoMolecular Integer;
  v_consulta_formula Moleculas.formula%TYPE;
  
  TAMAÑOS_INADECUADOS  exception;
  NOMBRE_YA_EXISTENTE exception;
  NO_EXISTE_ATOMO  exception;
  FORMULA_YA_EXISTENTE exception;
  
  PRAGMA EXCEPTION_INIT(TAMAÑOS_INADECUADOS, -20000);
  PRAGMA EXCEPTION_INIT(NOMBRE_YA_EXISTENTE, -20001);
  PRAGMA EXCEPTION_INIT(NO_EXISTE_ATOMO, -20002);
  PRAGMA EXCEPTION_INIT(FORMULA_YA_EXISTENTE, -20003);
  
  --v1 array_type;

begin
    
  if(p_simbolos.count != p_nros.count) then
    raise TAMAÑOS_INADECUADOS;
  end if;

  --Comprobamos que el nombre de la molecula a insertar existe
  SELECT count(nombre) into v_nombre_mol FROM Moleculas WHERE nombre=p_nombre;
  if(v_nombre_mol > 0) then
    raise NOMBRE_YA_EXISTENTE;
  end if;
  
  --Calcular la formula y sacar pesos.
  FOR i IN p_simbolos.FIRST .. p_simbolos.LAST
  LOOP
    --concatenamos la formula cada iteracion.
    v_formulaFinal := v_formulaFinal || p_simbolos(i);
    if(p_nros(i)>1) then
      v_formulaFinal := v_formulaFinal || p_nros(i);
    end if;
    
    --sacamos el pesoMolecular total.
    select pesoAtomico into v_pesoMolecular from Elementos Where simbolo=p_simbolos(i);
    v_pesoMolecularTotal := v_pesoMolecularTotal + v_pesoMolecular*p_nros(i);
  
  END LOOP;
  
  --Comprobamos que la formula ya existe.
  select count(formula) into v_consulta_formula from Moleculas where formula=v_formulaFinal;
  if(v_consulta_formula>0) then 
    raise FORMULA_YA_EXISTENTE;
  end if;
  
  --Una vez hechas las comprobaciones insertamos.
  
  --Insertamos en Moleculas.
  INSERT INTO Moleculas(id, nombre, pesoMolecular, formula) values(seq_molId.nextval, p_nombre, v_pesoMolecularTotal, v_formulaFinal);
  
  --Insertamos en composicion.
  
  FOR i IN p_simbolos.FIRST .. p_simbolos.LAST
  LOOP
    INSERT INTO Composicion(simbolo, idMolecula, nroAtomos) values(p_simbolos(i), seq_molId.currval, p_nros(i));
  END LOOP;
  
  commit;

exception
  when no_data_found then
    rollback;
    dbms_output.put_line('->Not found.');
  when TAMAÑOS_INADECUADOS then
    rollback;
    dbms_output.put_line('->La longitud de los arrays es diferente.');
  when NOMBRE_YA_EXISTENTE then
    rollback;
    dbms_output.put_line('->El nombre de la molecula ya existe.');
  when FORMULA_YA_EXISTENTE then
    rollback;
    dbms_output.put_line('->La formula de esa molecula ya existe.');

end;
/




--Poner el serveroutput a on
set serveroutput on

declare
  v1 Nested_type;
  v2 Nested_type;
begin

  dbms_output.put_line('Pruebas:');
  
  v1:=Nested_type();
  v1.extend(2);
  
  v1(1):='H';
  v1(2):='O';

  v2:=Nested_type();
  v2.extend(2);
  
  v2(1):=2;
  v2(2):=2;
 
  --borrarMoleculaId(1);
  --borrarMoleculaNombre('Agua');
  --actualizarMoleculaId(1,'O',2);
  --actualizarMoleculaNombre('Agua','O',2);
  --insertarMolecula('AguaOxigenada',v1,v2);
  
end;
/




exit;

--select * FROM Moleculas
--select * FROM Composicion
--select * FROM Elementos



