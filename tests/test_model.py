"""Test cases for cube model classes."""

import pytest

from cube_demo import Cube, Model, Relation


class TestCube:
    """Tests for the Cube class."""

    def test_create_cube_with_name(self):
        cube = Cube(name="users")
        assert cube.name == "users"
        assert cube.columns == []

    def test_create_cube_with_columns(self):
        cube = Cube(name="users", columns=["id", "name", "email"])
        assert cube.name == "users"
        assert cube.columns == ["id", "name", "email"]

    def test_cube_equality_by_name(self):
        cube1 = Cube(name="users", columns=["id"])
        cube2 = Cube(name="users", columns=["id", "name"])
        assert cube1 == cube2

    def test_cube_hash(self):
        cube1 = Cube(name="users")
        cube2 = Cube(name="users")
        assert hash(cube1) == hash(cube2)


class TestRelation:
    """Tests for the Relation class."""

    def test_create_relation(self):
        customers = Cube(name="customers", columns=["id", "name"])
        orders = Cube(name="orders", columns=["id", "customer_id", "total"])

        relation = Relation(
            left_cube=orders,
            right_cube=customers,
            left_column="customer_id",
            right_column="id",
        )

        assert relation.left_cube == orders
        assert relation.right_cube == customers
        assert relation.left_column == "customer_id"
        assert relation.right_column == "id"

    def test_relation_label(self):
        customers = Cube(name="customers", columns=["id", "name"])
        orders = Cube(name="orders", columns=["id", "customer_id"])

        relation = Relation(
            left_cube=orders,
            right_cube=customers,
            left_column="customer_id",
            right_column="id",
        )

        assert relation.label == "orders.customer_id → customers.id"

    def test_relation_invalid_left_column(self):
        customers = Cube(name="customers", columns=["id", "name"])
        orders = Cube(name="orders", columns=["id", "customer_id"])

        with pytest.raises(ValueError, match="Column 'invalid' not found in cube 'orders'"):
            Relation(
                left_cube=orders,
                right_cube=customers,
                left_column="invalid",
                right_column="id",
            )

    def test_relation_invalid_right_column(self):
        customers = Cube(name="customers", columns=["id", "name"])
        orders = Cube(name="orders", columns=["id", "customer_id"])

        with pytest.raises(ValueError, match="Column 'invalid' not found in cube 'customers'"):
            Relation(
                left_cube=orders,
                right_cube=customers,
                left_column="customer_id",
                right_column="invalid",
            )


class TestModel:
    """Tests for the Model class."""

    def test_create_empty_model(self):
        model = Model(name="ecommerce")
        assert model.name == "ecommerce"
        assert model.cubes == {}
        assert model.relations == set()

    def test_add_cube(self):
        model = Model()
        cube = Cube(name="users", columns=["id", "name"])
        model.add_cube(cube)

        assert "users" in model.cubes
        assert model.cubes["users"] == cube

    def test_add_duplicate_cube_raises_error(self):
        model = Model()
        cube1 = Cube(name="users", columns=["id"])
        cube2 = Cube(name="users", columns=["id", "name"])

        model.add_cube(cube1)
        with pytest.raises(ValueError, match="Cube 'users' already exists"):
            model.add_cube(cube2)

    def test_get_cube(self):
        model = Model()
        cube = Cube(name="users", columns=["id"])
        model.add_cube(cube)

        assert model.get_cube("users") == cube

    def test_get_cube_not_found(self):
        model = Model()
        with pytest.raises(KeyError, match="Cube 'users' not found"):
            model.get_cube("users")

    def test_add_relation(self):
        model = Model()
        customers = Cube(name="customers", columns=["id", "name"])
        orders = Cube(name="orders", columns=["id", "customer_id"])

        model.add_cube(customers)
        model.add_cube(orders)

        relation = Relation(
            left_cube=orders,
            right_cube=customers,
            left_column="customer_id",
            right_column="id",
        )
        model.add_relation(relation)

        assert len(model.relations) == 1
        assert relation in model.relations

    def test_add_relation_missing_left_cube(self):
        model = Model()
        customers = Cube(name="customers", columns=["id"])
        orders = Cube(name="orders", columns=["id", "customer_id"])

        model.add_cube(customers)  # Only add customers, not orders

        relation = Relation(
            left_cube=orders,
            right_cube=customers,
            left_column="customer_id",
            right_column="id",
        )

        with pytest.raises(ValueError, match="Left cube 'orders' not found"):
            model.add_relation(relation)

    def test_to_graph_data(self):
        model = Model(name="ecommerce")

        customers = Cube(name="customers", columns=["id", "name", "email"])
        orders = Cube(name="orders", columns=["id", "customer_id", "total"])
        products = Cube(name="products", columns=["id", "name", "price"])

        model.add_cube(customers)
        model.add_cube(orders)
        model.add_cube(products)

        model.add_relation(
            Relation(
                left_cube=orders,
                right_cube=customers,
                left_column="customer_id",
                right_column="id",
            )
        )

        graph_data = model.to_graph_data()

        assert len(graph_data["nodes"]) == 3
        assert len(graph_data["edges"]) == 1

        # Check nodes
        node_names = [n["label"] for n in graph_data["nodes"]]
        assert "customers" in node_names
        assert "orders" in node_names
        assert "products" in node_names

        # Check edge
        edge = graph_data["edges"][0]
        assert edge["source"] == "orders"
        assert edge["target"] == "customers"
        assert edge["label"] == "customer_id → id"


class TestIntegration:
    """Integration tests with a complete model."""

    def test_ecommerce_model(self):
        """Test a complete e-commerce model with multiple cubes and relations."""
        model = Model(name="E-Commerce")

        # Create cubes
        customers = Cube(
            name="customers",
            columns=["id", "name", "email", "created_at"],
        )
        orders = Cube(
            name="orders",
            columns=["id", "customer_id", "order_date", "total", "status"],
        )
        order_items = Cube(
            name="order_items",
            columns=["id", "order_id", "product_id", "quantity", "price"],
        )
        products = Cube(
            name="products",
            columns=["id", "name", "category", "price", "stock"],
        )

        # Add cubes to model
        for cube in [customers, orders, order_items, products]:
            model.add_cube(cube)

        # Create relations
        model.add_relation(
            Relation(orders, customers, "customer_id", "id")
        )
        model.add_relation(
            Relation(order_items, orders, "order_id", "id")
        )
        model.add_relation(
            Relation(order_items, products, "product_id", "id")
        )

        # Verify model structure
        assert len(model.cubes) == 4
        assert len(model.relations) == 3

        # Verify graph export
        graph_data = model.to_graph_data()
        assert len(graph_data["nodes"]) == 4
        assert len(graph_data["edges"]) == 3

