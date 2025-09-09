import Knex from 'knex';
import fs from 'fs';
import path from 'path';

// Database connection (same as in migrate.js)
const connection = 'postgres://postgres:admin123@localhost:5432/mongo_test';

async function setupDatabase() {
    console.log('Setting up PostgreSQL database schema...');

    let knex;
    try {
        knex = Knex({
            client: 'pg',
            connection: connection
        });
        console.log('Connected to PostgreSQL');
    } catch (err) {
        console.error('ERROR connecting to PostgreSQL:', err);
        process.exit(1);
    }

    try {
        // Create tables individually
        console.log('Creating departments table...');
        await knex.schema.createTableIfNotExists('departments', (table) => {
            table.increments('id').primary();
            table.string('name');
            table.integer('dep_type');
            table.timestamp('created_at');
            table.timestamp('updated_at');
        });

        console.log('Creating awards table...');
        await knex.schema.createTableIfNotExists('awards', (table) => {
            table.increments('id').primary();
            table.string('name');
            table.timestamp('created_at');
            table.timestamp('updated_at');
        });

        console.log('Creating employees table...');
        await knex.schema.createTableIfNotExists('employees', (table) => {
            table.increments('id').primary();
            table.string('name');
            table.integer('department').references('id').inTable('departments');
            table.timestamp('created_at');
            table.timestamp('updated_at');
        });

        console.log('Creating emplyees__awards table...');
        await knex.schema.createTableIfNotExists('emplyees__awards', (table) => {
            table.increments('id').primary();
            table.integer('employee_id').references('id').inTable('employees');
            table.integer('award_id').references('id').inTable('awards');
        });

        console.log('Database schema created successfully!');
        console.log('Tables created:');
        console.log('- departments');
        console.log('- awards');
        console.log('- employees');
        console.log('- emplyees__awards (many-to-many)');

    } catch (err) {
        console.error('ERROR creating database schema:', err);
        console.log('\nIf you get permission errors, you may need to:');
        console.log('1. Connect to PostgreSQL as a superuser (postgres)');
        console.log('2. Grant CREATE privileges to the migrator user:');
        console.log('   GRANT CREATE ON SCHEMA public TO migrator;');
        console.log('3. Or create the tables manually using the create-tables.sql file');
        process.exit(1);
    } finally {
        await knex.destroy();
    }
}

setupDatabase()
    .then(() => {
        console.log('Database setup completed successfully!');
        process.exit(0);
    })
    .catch((err) => {
        console.error('Database setup failed:', err);
        process.exit(1);
    });
